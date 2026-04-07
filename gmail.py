import base64
import mimetypes
import re
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


class GmailService:
    def __init__(self, credentials: Credentials, account_name: str = ""):
        self.service = build("gmail", "v1", credentials=credentials)
        self.account_name = account_name

    # ------------------------------------------------------------------ profile

    def get_profile(self) -> Dict[str, Any]:
        return self.service.users().getProfile(userId="me").execute()

    # ------------------------------------------------------------------ search / read

    def search_messages(
        self,
        query: str,
        max_results: int = 20,
        page_token: Optional[str] = None,
        include_body: bool = False,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "userId": "me",
            "q": query,
            "maxResults": min(max_results, 100),
        }
        if page_token:
            params["pageToken"] = page_token

        result = self.service.users().messages().list(**params).execute()
        raw_messages = result.get("messages", [])

        messages = []
        for raw in raw_messages:
            fmt = "full" if include_body else "metadata"
            msg = self._get_raw_message(raw["id"], format=fmt)
            messages.append(self._parse_message(msg))

        return {
            "messages": messages,
            "nextPageToken": result.get("nextPageToken"),
            "resultSizeEstimate": result.get("resultSizeEstimate", 0),
        }

    def get_message(self, message_id: str) -> Dict[str, Any]:
        msg = self._get_raw_message(message_id, format="full")
        return self._parse_message(msg)

    def get_thread(self, thread_id: str) -> Dict[str, Any]:
        thread = self.service.users().threads().get(userId="me", id=thread_id).execute()
        messages = [self._parse_message(m) for m in thread.get("messages", [])]
        return {
            "id": thread["id"],
            "messageCount": len(messages),
            "messages": messages,
        }

    # ------------------------------------------------------------------ attachments

    def get_attachment(
        self, message_id: str, attachment_id: str, filename: str, save_dir: Path
    ) -> Path:
        data = (
            self.service.users()
            .messages()
            .attachments()
            .get(userId="me", messageId=message_id, id=attachment_id)
            .execute()
        )
        file_data = base64.urlsafe_b64decode(data["data"])
        save_path = save_dir / filename
        # Avoid overwriting existing files by appending a number
        counter = 1
        while save_path.exists():
            stem = Path(filename).stem
            suffix = Path(filename).suffix
            save_path = save_dir / f"{stem}_{counter}{suffix}"
            counter += 1
        save_path.write_bytes(file_data)
        return save_path

    # ------------------------------------------------------------------ send / draft

    @staticmethod
    def _strip_html(html: str) -> str:
        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
        text = re.sub(r"</(p|div|tr|li|h[1-6])>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        text = (
            text.replace("&nbsp;", " ")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
            .replace("&quot;", '"')
        )
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _build_message(
        self,
        to: str,
        subject: str,
        body: str,
        cc: str = "",
        bcc: str = "",
        attachment_paths: Optional[List[str]] = None,
        body_type: str = "html",
    ) -> MIMEText | MIMEMultipart:
        has_attachments = bool(attachment_paths)
        is_html = body_type != "plain"

        if is_html:
            plain_text = self._strip_html(body)
            if has_attachments:
                msg = MIMEMultipart("mixed")
                alt_part = MIMEMultipart("alternative")
                alt_part.attach(MIMEText(plain_text, "plain"))
                alt_part.attach(MIMEText(body, "html"))
                msg.attach(alt_part)
            else:
                msg = MIMEMultipart("alternative")
                msg.attach(MIMEText(plain_text, "plain"))
                msg.attach(MIMEText(body, "html"))
        else:
            if has_attachments:
                msg = MIMEMultipart("mixed")
                msg.attach(MIMEText(body, "plain"))
            else:
                msg = MIMEText(body, "plain")

        if has_attachments:
            for file_path in attachment_paths:
                path = Path(file_path)
                if not path.is_file():
                    raise ValueError(f"Attachment not found: {file_path}")
                content_type, _ = mimetypes.guess_type(str(path))
                if content_type is None:
                    content_type = "application/octet-stream"
                main_type, sub_type = content_type.split("/", 1)
                part = MIMEBase(main_type, sub_type)
                part.set_payload(path.read_bytes())
                from email import encoders
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition", "attachment", filename=path.name
                )
                msg.attach(part)

        msg["to"] = to
        msg["subject"] = subject
        if cc:
            msg["cc"] = cc
        if bcc:
            msg["bcc"] = bcc
        return msg

    def reply(
        self,
        message_id: str,
        body: str,
        cc: str = "",
        bcc: str = "",
        attachment_paths: Optional[List[str]] = None,
        draft: bool = False,
    ) -> Dict[str, Any]:
        original = self.get_message(message_id)
        thread_id = original["threadId"]
        orig_message_id = original["messageId"]
        orig_references = original["references"]
        orig_subject = original["subject"]
        orig_from = original["from"]

        # Build References chain
        references = f"{orig_references} {orig_message_id}".strip()

        # Ensure subject has Re: prefix
        subject = orig_subject if orig_subject.lower().startswith("re:") else f"Re: {orig_subject}"

        msg = self._build_message(
            to=orig_from, subject=subject, body=body,
            cc=cc, bcc=bcc, attachment_paths=attachment_paths,
        )
        msg["In-Reply-To"] = orig_message_id
        msg["References"] = references

        message_body = {"raw": self._encode(msg), "threadId": thread_id}
        if draft:
            return self.service.users().drafts().create(
                userId="me", body={"message": message_body}
            ).execute()
        return self.service.users().messages().send(
            userId="me", body=message_body,
        ).execute()

    def forward(
        self,
        message_id: str,
        to: str,
        body: str = "",
        cc: str = "",
        bcc: str = "",
        draft: bool = False,
    ) -> Dict[str, Any]:
        original = self.get_message(message_id)
        orig_subject = original["subject"]
        orig_from = original["from"]
        orig_date = original["date"]
        orig_to = original["to"]
        orig_body_html = original.get("body_html") or ""
        orig_body_plain = original.get("body_plain") or original.get("body") or ""

        subject = orig_subject if orig_subject.lower().startswith("fwd:") else f"Fwd: {orig_subject}"

        def _esc(s: str) -> str:
            return (
                s.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
            )

        # Build HTML forward: preserve original HTML inside a Gmail-style blockquote.
        forwarded_header_html = (
            "<br><br><div class=\"gmail_quote\">"
            "---------- Forwarded message ----------<br>"
            f"From: {_esc(orig_from)}<br>"
            f"Date: {_esc(orig_date)}<br>"
            f"Subject: {_esc(orig_subject)}<br>"
            f"To: {_esc(orig_to)}<br><br>"
        )
        if orig_body_html:
            original_html_block = orig_body_html
        else:
            # No original HTML available — escape the plain text and use <pre> to preserve whitespace.
            original_html_block = f"<pre style=\"white-space: pre-wrap; font-family: inherit;\">{_esc(orig_body_plain)}</pre>"
        full_body_html = f"{body}{forwarded_header_html}{original_html_block}</div>"

        # Plain-text alternative: use the original's plain text (or a stripped version of the HTML).
        user_body_plain = self._strip_html(body) if body else ""
        orig_plain_for_alt = orig_body_plain or (self._html_to_readable(orig_body_html) if orig_body_html else "")
        full_body_plain = (
            f"{user_body_plain}\n\n"
            "---------- Forwarded message ----------\n"
            f"From: {orig_from}\n"
            f"Date: {orig_date}\n"
            f"Subject: {orig_subject}\n"
            f"To: {orig_to}\n\n"
            f"{orig_plain_for_alt}"
        )

        # Fetch original attachments in memory and attach them
        raw_msg = self._get_raw_message(message_id, format="full")
        attachment_parts = self._get_attachment_parts(raw_msg)

        msg = MIMEMultipart("mixed")
        alt_part = MIMEMultipart("alternative")
        alt_part.attach(MIMEText(full_body_plain, "plain"))
        alt_part.attach(MIMEText(full_body_html, "html"))
        msg.attach(alt_part)

        for apart in attachment_parts:
            msg.attach(apart)

        msg["to"] = to
        msg["subject"] = subject
        if cc:
            msg["cc"] = cc
        if bcc:
            msg["bcc"] = bcc

        message_body = {"raw": self._encode(msg)}
        if draft:
            return self.service.users().drafts().create(
                userId="me", body={"message": message_body}
            ).execute()
        return self.service.users().messages().send(
            userId="me", body=message_body
        ).execute()

    def send_message(
        self,
        to: str,
        subject: str,
        body: str,
        cc: str = "",
        bcc: str = "",
        attachment_paths: Optional[List[str]] = None,
        draft: bool = False,
        body_type: str = "html",
    ) -> Dict[str, Any]:
        msg = self._build_message(to, subject, body, cc, bcc, attachment_paths, body_type=body_type)
        message_body = {"raw": self._encode(msg)}
        if draft:
            return self.service.users().drafts().create(
                userId="me", body={"message": message_body}
            ).execute()
        return self.service.users().messages().send(
            userId="me", body=message_body
        ).execute()

    def create_draft(
        self,
        to: str,
        subject: str,
        body: str,
        cc: str = "",
        bcc: str = "",
        attachment_paths: Optional[List[str]] = None,
        body_type: str = "html",
    ) -> Dict[str, Any]:
        msg = self._build_message(to, subject, body, cc, bcc, attachment_paths, body_type=body_type)
        return self.service.users().drafts().create(
            userId="me", body={"message": {"raw": self._encode(msg)}}
        ).execute()

    def list_drafts(self, max_results: int = 20) -> List[Dict[str, Any]]:
        result = self.service.users().drafts().list(
            userId="me", maxResults=min(max_results, 50)
        ).execute()

        drafts = []
        for draft in result.get("drafts", []):
            details = self.service.users().drafts().get(
                userId="me", id=draft["id"], format="full"
            ).execute()
            msg = self._parse_message(details.get("message", {}))
            msg["draft_id"] = draft["id"]
            drafts.append(msg)

        return drafts

    # ------------------------------------------------------------------ labels

    def list_labels(self) -> List[Dict[str, Any]]:
        result = self.service.users().labels().list(userId="me").execute()
        return [
            {"id": lbl["id"], "name": lbl["name"], "type": lbl.get("type", "")}
            for lbl in result.get("labels", [])
        ]

    def create_label(self, name: str) -> Dict[str, Any]:
        label = self.service.users().labels().create(
            userId="me",
            body={
                "name": name,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            },
        ).execute()
        return {"id": label["id"], "name": label["name"], "type": label.get("type", "")}

    def modify_labels(
        self,
        message_id: str,
        add_labels: Optional[List[str]] = None,
        remove_labels: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if add_labels:
            body["addLabelIds"] = add_labels
        if remove_labels:
            body["removeLabelIds"] = remove_labels

        return self.service.users().messages().modify(
            userId="me", id=message_id, body=body
        ).execute()

    def archive_message(self, message_id: str) -> Dict[str, Any]:
        return self.modify_labels(message_id, remove_labels=["INBOX"])

    def modify_thread_labels(
        self,
        thread_id: str,
        add_labels: Optional[List[str]] = None,
        remove_labels: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if add_labels:
            body["addLabelIds"] = add_labels
        if remove_labels:
            body["removeLabelIds"] = remove_labels

        return self.service.users().threads().modify(
            userId="me", id=thread_id, body=body
        ).execute()

    def archive_thread(self, thread_id: str) -> Dict[str, Any]:
        return self.modify_thread_labels(thread_id, remove_labels=["INBOX"])

    # ------------------------------------------------------------------ internals

    def _get_raw_message(self, message_id: str, format: str = "full") -> Dict[str, Any]:
        return self.service.users().messages().get(
            userId="me", id=message_id, format=format
        ).execute()

    def _parse_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        payload = msg.get("payload", {})
        headers: Dict[str, str] = {}
        for h in payload.get("headers", []):
            headers[h["name"].lower()] = h["value"]

        parts = self._extract_body_parts(payload)
        body_html = parts.get("html", "")
        body_plain = parts.get("plain", "")
        # Human-readable body: prefer stripped HTML if present, else plain text.
        if body_html:
            body = self._html_to_readable(body_html)
        else:
            body = body_plain
        attachments = self._extract_attachments(payload)

        return {
            "id": msg.get("id", ""),
            "threadId": msg.get("threadId", ""),
            "labels": msg.get("labelIds", []),
            "snippet": msg.get("snippet", ""),
            "date": headers.get("date", ""),
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "cc": headers.get("cc", ""),
            "subject": headers.get("subject", "(no subject)"),
            "messageId": headers.get("message-id", ""),
            "references": headers.get("references", ""),
            "body": body,
            "body_html": body_html,
            "body_plain": body_plain,
            "attachments": attachments,
        }

    def _get_attachment_parts(self, raw_msg: Dict[str, Any]) -> List[MIMEBase]:
        """Fetch attachment data from the API and return as MIME parts for forwarding."""
        from email import encoders

        payload = raw_msg.get("payload", {})
        msg_id = raw_msg.get("id", "")
        attachment_meta = self._extract_attachments(payload)
        parts = []
        for att in attachment_meta:
            data = (
                self.service.users()
                .messages()
                .attachments()
                .get(userId="me", messageId=msg_id, id=att["attachmentId"])
                .execute()
            )
            file_data = base64.urlsafe_b64decode(data["data"])
            content_type = att.get("mimeType", "application/octet-stream")
            main_type, sub_type = content_type.split("/", 1)
            part = MIMEBase(main_type, sub_type)
            part.set_payload(file_data)
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition", "attachment", filename=att["filename"]
            )
            parts.append(part)
        return parts

    def _extract_attachments(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        attachments = []
        parts = payload.get("parts", [])
        for part in parts:
            filename = part.get("filename", "")
            body = part.get("body", {})
            if filename and body.get("attachmentId"):
                attachments.append({
                    "attachmentId": body["attachmentId"],
                    "filename": filename,
                    "mimeType": part.get("mimeType", ""),
                    "size": body.get("size", 0),
                })
            # Recurse into nested multipart parts
            attachments.extend(self._extract_attachments(part))
        return attachments

    def _extract_body_parts(self, payload: Dict[str, Any]) -> Dict[str, str]:
        """Walk the MIME tree and return the first text/html and text/plain payloads found.

        Returns {"html": str, "plain": str}. Either may be empty.
        Attachment parts (those with a filename) are skipped so that e.g. an HTML
        attachment doesn't get picked up as the body.
        """
        result = {"html": "", "plain": ""}
        if not payload:
            return result

        def walk(part: Dict[str, Any]) -> None:
            mime_type = part.get("mimeType", "")
            body = part.get("body", {})
            filename = part.get("filename", "") or ""
            data = body.get("data", "")

            if data and not filename:
                decoded = base64.urlsafe_b64decode(data.encode()).decode(
                    "utf-8", errors="replace"
                )
                if mime_type == "text/html" and not result["html"]:
                    result["html"] = decoded
                elif mime_type == "text/plain" and not result["plain"]:
                    result["plain"] = decoded

            for child in part.get("parts", []) or []:
                walk(child)

        walk(payload)
        return result

    @staticmethod
    def _html_to_readable(html: str) -> str:
        """Convert HTML into a readable plain-text representation for LLM consumption."""
        decoded = re.sub(
            r"<style\b[^>]*>.*?</style>", " ", html,
            flags=re.DOTALL | re.IGNORECASE,
        )
        decoded = re.sub(
            r"<script\b[^>]*>.*?</script>", " ", decoded,
            flags=re.DOTALL | re.IGNORECASE,
        )
        decoded = re.sub(r"<[^>]+>", " ", decoded)
        decoded = (
            decoded.replace("&nbsp;", " ")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
            .replace("&quot;", '"')
            .replace("&apos;", "'")
            .replace("&#39;", "'")
            .replace("&#160;", " ")
            .replace("&#8203;", "")
        )
        decoded = re.sub(
            r"&#(\d+);",
            lambda m: chr(int(m.group(1))) if int(m.group(1)) < 0x110000 else m.group(0),
            decoded,
        )
        decoded = re.sub(
            r"&#[xX]([0-9a-fA-F]+);",
            lambda m: chr(int(m.group(1), 16)) if int(m.group(1), 16) < 0x110000 else m.group(0),
            decoded,
        )
        decoded = re.sub(r"\s+", " ", decoded)
        return decoded.strip()

    def _extract_body(self, payload: Dict[str, Any]) -> str:
        """Backward-compat shim: return a readable text version of the body."""
        parts = self._extract_body_parts(payload)
        if parts["html"]:
            return self._html_to_readable(parts["html"])
        return parts["plain"].strip()

    @staticmethod
    def _encode(msg: MIMEText | MIMEMultipart) -> str:
        return base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

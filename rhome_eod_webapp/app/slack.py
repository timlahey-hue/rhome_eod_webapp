import requests

def share_summary(webhook_url: str, summary_text: str, blocks=None):
  payload = {"text": summary_text}
  if blocks:
    payload["blocks"] = blocks
  r = requests.post(webhook_url, json=payload, timeout=15)
  if r.status_code >= 300:
    raise RuntimeError(f"Slack webhook error {r.status_code}: {r.text}")

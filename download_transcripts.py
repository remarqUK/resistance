"""Download YouTube transcripts for NickShawnFX videos."""
import json, os
from youtube_transcript_api import YouTubeTranscriptApi

VIDEOS = [
    ("6SyPsKlloHk", "$30,000 Was Stolen From Me Trading Forex"),
    ("OitaoqWH_LI", "How I Make $50K To $100K/Month Trading Forex"),
    ("dTjYuvfLdsQ", "The ONLY Trading Strategy You'll Ever Need"),
    ("9rY9u7-vi0s", "The Biggest Forex Trade Of My Career"),
    ("plYArTAgG8k", "My Entire Support/Resistance Trading Strategy!"),
    ("LnwmqYJ7CiQ", "You've Been Lied To About Risk/Reward In Trading"),
    ("dk7HekXcpKI", "I'm Going To Try To Make $100,000 Trading Forex This Week"),
    ("INsu8f-AU-I", "Your Trading Strategy Is Stupid"),
    ("_JqCempbp6E", "The BEST Way I've Found To Make Forex Trading Work"),
    ("K7TaHWDrbh8", "Should You Sell At Support, & Buy At Resistance?"),
    ("NSLYDWYe7Ts", "My First Live Trading Session With MFX Clients IN A YEAR!!!"),
    ("-v2jL4w-iJo", "How To Grow $100 To $1,000 Trading Forex! (Realistically)"),
    ("lg345wRhbsY", "Reacting To Your Forex Questions I'm Afraid To Answer!"),
    ("MO0C_W-t838", "This Forex Trading Strat Made Me A Millionaire (As An Idiot)"),
    ("_KQRL1Avq_o", "Trading Forex Used To Be HARD AF, Until I Realized This..."),
    ("d5R7oGVSHWQ", "I've Traded Forex For 10 Years. What Actually Matters?"),
    ("DOpFr2ugiwo", "This Trading Strategy Made Me Millions (Backtesting It)"),
    ("tVOHQ6e1mhQ", "I've Been Trading For 10 Years, This Is Why People Fail"),
    ("7X_uP9srJIc", "How To Trade Correlated Forex Pairs Without Losing Your A$$"),
    ("q03yNlBdtqM", "This Exact Forex Trading Setup Made Me Millions"),
]

OUT_DIR = "transcripts"
os.makedirs(OUT_DIR, exist_ok=True)

api = YouTubeTranscriptApi()
success = 0
failed = []

for vid_id, title in VIDEOS:
    safe_title = "".join(c if c.isalnum() or c in " -_" else "" for c in title).strip()
    filename = f"{vid_id}_{safe_title[:60]}.json"
    filepath = os.path.join(OUT_DIR, filename)
    try:
        transcript = api.fetch(vid_id)
        raw_data = transcript.to_raw_data()
        full_text = " ".join(seg["text"] for seg in raw_data)
        data = {
            "video_id": vid_id,
            "title": title,
            "url": f"https://www.youtube.com/watch?v={vid_id}",
            "language": transcript.language,
            "is_generated": transcript.is_generated,
            "transcript_segments": raw_data,
            "full_text": full_text,
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        success += 1
        print(f"  [{success:>2}/20] {title[:70]}")
    except Exception as e:
        failed.append((vid_id, title, str(e)))
        print(f"  [FAIL] {title[:70]}: {e}")

print(f"\nDone: {success} downloaded, {len(failed)} failed")
if failed:
    print("Failed:")
    for vid_id, title, err in failed:
        print(f"  {vid_id}: {err[:100]}")

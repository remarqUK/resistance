import json

with open('C:/Users/mouse/OneDrive/TMP/Resistance/all_transcripts.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

target_ids = ['NSLYDWYe7Ts', '-v2jL4w-iJo', 'lg345wRhbsY']

for vid_id in target_ids:
    if vid_id in data:
        entry = data[vid_id]
        out_path = f'C:/Users/mouse/OneDrive/TMP/Resistance/transcript_{vid_id.replace("-","_")}.txt'
        with open(out_path, 'w', encoding='utf-8') as out:
            out.write(f"TITLE: {entry['title']}\n\n")
            out.write(entry['text'])
        print(f"Written: {out_path}")
    else:
        print(f"NOT FOUND: {vid_id}")

from pathlib import Path

p = Path("pdv_web_full.py")
s = p.read_text(encoding="utf-8")

s = s.replace("use_container_width=True", 'width="stretch"')
s = s.replace("use_container_width=False", 'width="content"')

p.write_text(s, encoding="utf-8")
print("OK: substituído use_container_width por width")

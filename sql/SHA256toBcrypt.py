import bcrypt
import random
import string

# Generates a random readable password and its bcrypt hash
def make_entry(user_id):
    pwd = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
    hashed = bcrypt.hashpw(pwd.encode(), bcrypt.gensalt(rounds=10)).decode()
    return user_id, pwd, hashed

# Pull your actual user_ids first, adjust range or swap in a real ID list
user_ids = range(1, 10001)  # whatever your actual range is

lines = []
for uid in user_ids:
    uid, pwd, hashed = make_entry(uid)
    hashed = hashed.replace("'", "''")
    lines.append(f"UPDATE users SET password = '{hashed}' WHERE user_id = {uid};")
    if uid % 100 == 0:
        print(f"  {uid} / {len(user_ids)} done...")

with open("update_passwords.sql", "w") as f:
    f.write("\n".join(lines))

print("Done — update_passwords.sql generated")
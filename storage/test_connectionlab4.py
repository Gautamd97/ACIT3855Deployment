from database import make_session

try:
    s = make_session()
    print("Connected successfully:", s.bind)
    s.close()
except Exception as e:
    print("Connection failed:", e)

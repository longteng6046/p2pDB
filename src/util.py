import hashlib

if __name__ == "__main__":
    print len(hashlib.sha1("hello").hexdigest())
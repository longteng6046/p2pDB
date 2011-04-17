import hashlib, binascii

def formalize(ip):
    tokens = ip.split(".")
    assert len(tokens)==4
    
    for i in xrange(len(tokens)):
        for j in xrange(3-len(tokens[i])):
            tokens[i] = "0"+tokens[i]
                    
    return ".".join(tokens)
    
if __name__ == "__main__":
    print formalize("12.52.126.25")
def getIP(ifname):
    import socket, fcntl, struct

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

def formalize(ip):
    tokens = ip.split(".")
    assert len(tokens)==4
    
    for i in xrange(len(tokens)):
        assert len(tokens[i])<=3 and len(tokens[i])>0
        for j in xrange(3-len(tokens[i])):
            tokens[i] = "0"+tokens[i]
                    
    return ".".join(tokens)

def getExpression(ip):
    ip = formalize(ip)
    return "".join(ip.split("."))

def findClosestIP(target_ip, node_dict):
    closest_node = None
    # get the largest possible difference
    closest_distance = 999999999
        
    for id in node_dict.keys():
        if id==None:
            continue
        ip = node_dict[id]
        current_distance = abs(compare(getExpression(ip), getExpression(target_ip)))
        if current_distance <= closest_distance:
            closest_distance = current_distance
            closest_node = (id, node_dict[id]) 
    
    return closest_node

def findFurthestIP(target_ip, node_dict):
    furthest_node = None
    furthest_distance = 0
        
    for id in node_dict:
        if id==None:
            continue
        current_distance = abs(compare(getExpression(ip), getExpression(target_ip)))
        if current_distance <= furthest_distance:
            furthest_distance = current_distance
            furthest_node = (id, node_dict[id])
    
    return furthest_node

if __name__ == "__main__":
    print formalize("12.52.126.25")
    print getExpression("12.52.421.54")
     
    print getIP("eth0")
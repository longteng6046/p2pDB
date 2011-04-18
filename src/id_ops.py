import hashlib, binascii

# get sha-1 hash of a string s
def getSha1(s):
    return hashlib.sha1(s).hexdigest()
    
# s: a non-negative integer
# @return: the bit string representation of s
# @deprecated: please use bit string operation instead
def getBitString(s):
    if s <= 1:
        return str(s)
    else:
        return getBitString(s>>1) + str(s&1)

# s: a non-negative integer
# maxLength: the maximum length of the retrieved bit string
# @return: the bit string representation of s
# @deprecated: please use bit string operation instead
def getBitStringToLength(s, maxLength):
    bs = getBitString(s)
    
    assert len(bs) <= maxLength
    
    for i in xrange(maxLength-len(bs)):
        bs = "0" + bs
     
    return bs

def expandLeftToLength(bit_str, max_length):
    for i in xrange(max_length-len(bit_str)):
        bit_str = "0"+bit_str
    return bit_str
            
def getCommonMSB(str_1, str_2, max_length=0):
    # make sure str_1 is greater than str_2 in length
    if len(str_1)<len(str_2):
        temp = str_1;
        str_1 = str_2;
        str_2 = temp;
    
    for i in xrange(len(str_1)-len(str_2)):
        str_2 = "0" + str_2
        
    assert len(str_1) == len(str_2)
    
    common_bit_string=""
    for i in xrange(max_length-len(str_1)):
        common_bit_string += "0"
        
    for i in xrange(len(str_1)):
        if str_1[i] == str_2[i]:
            common_bit_string += str_1[i]
        else:
            break
    
    return common_bit_string

def getHexDifference(hex_str_1, hex_str_2):
    assert len(hex_str_1)==len(hex_str_2)
    return getDifference(hex2bin(hex_str_1), hex2bin(hex_str_2))

def getDifference(bit_str_1, bit_str_2):
    bit_str_1 = reverseString(bit_str_1)
    bit_str_2 = reverseString(bit_str_2)
    
    if len(bit_str_1)<len(bit_str_2):
        for i in xrange(len(bit_str_2)-len(bit_str_1)):
            bit_str_1 += "0"
    elif len(bit_str_1)>len(bit_str_2):
        for i in xrange(len(bit_str_1)-len(bit_str_2)):
            bit_str_2 += "0"
    #print bit_str_1, "\t", bit_str_2

    diff = 0
    for i in xrange(len(bit_str_1)):
        if int(bit_str_1[i])>int(bit_str_2[i]):
            diff += 2**i
        elif int(bit_str_1[i])<int(bit_str_2[i]):
            diff -= 2**i
        else:
            continue;
    
    return diff

def reverseString(str):
    str = str[::-1]
    return str

def hex2bin(hex_str):
    bin = ['0000','0001','0010','0011','0100','0101','0110','0111','1000','1001','1010','1011','1100','1101','1110','1111']
    aa = ''
    for i in range(len(hex_str)):
        aa += bin[int(hex_str[i],base=16)]
    return aa

def findClosestID(target_id, node_dict):
    closest_node = None
    closest_distance = 2**(4**self.length)
        
    for id in node_dict.keys():
        if id==None:
            continue
        current_distance = abs(getHexDifference(id, target_id))
        if current_distance <= closest_distance:
            closest_distance = current_distance
            closest_node = (id, node_dict[id]) 
    
    return closest_node

def findFurthestID(target_id, node_dict):
    furthest_node = None
    furthest_distance = 0
        
    for id in node_dict:
        if id==None:
            continue
        current_distance = abs(getHexDifference(id, target_id))
        if current_distance >= furthest_distance:
            furthest_distance = current_distance
            furthest_node = (id, node_dict[id])
    
    return furthest_node

if __name__ == "__main__":
    print getSha1("hello, how are you doing?")
    print len(getSha1("hello, how are you doing?"))
    print hex2bin("1a1"), "\t", hex2bin('b2')
    print getCommonMSB(hex2bin('a1'), hex2bin('b2'), 15)
    print getDifference(hex2bin('a1'), hex2bin('b2'))
    print getHexDifference('b2', 'a1')

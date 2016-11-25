import operator
from collections import defaultdict
ost_KB_avail = defaultdict(list)
cur = 1000
pred = [500, 1000, 600, 700]
#assuming the first value in the array is the current one and the rest as predicted
#Convert array into an object of key value pairs.  The value associated with key 1 is always the current request
req = {1: 1000, 2: 500, 3: 1000, 4: 600, 5: 700}
#Let there be two OSSs
oss_load = {1: 50, 2: 20}

#Let there be 4 OSTs under each OSS
ost_KB_avail = {1:[4000, 2000, 300, 3000] , 2:[5000, 1000, 300, 700]}

#While sorting OSS and OSTs we should not lose their position, consider treating them as key:value pairs and sort them based on values so that we do not lose keys
#Sorting requests should be straight-forward
#There seems to be an issue even here, if we sort in a straigh-forward way on requests we will lose information about current request, we need to know to which OST current req got mapped to
#So treat everything as key value pairs and then have a mapping
req_sorted = sorted(req.items(), key=operator.itemgetter(1))
oss_load_sorted = sorted(oss_load.items(), key=operator.itemgetter(1))
ost_KB_avail_sorted = sorted(ost_KB_avail.items(), key=operator.itemgetter(1))
print req_sorted
print oss_load_sorted
print ost_KB_avail_sorted

#Pick the best possible OST based on load and have a mapping.  Once an OST has been selected move it to picked_OST list, do this till there are no more OSTs left.  
#After this make picked_OST as freed_OST and then select the OST
#Suppose the same OST which is in picked list is the best possible OST then assign that itself
#In case of a tie choose the one with the lowest index

#we can start the mapping looking only at req and oss_load.
#So which index position of OSS corressponds with the req, what if we have more requests then OSSs then how do we find one-one mapping
#what if it is the other way around, what if there are more OSS then req, I think this case is pretty straight-forward
#can we take a decision to choose only that many predicted requests as there are OSSs
#Also we still havent taken stripe count into account
#Depending upon stripe count we will have to choose OSTs

least_load = min(oss_load, key=oss_load.get)
print least_load

#Keep sorting each time, this is the easiest way to implement
#So now I know the least loaded OSS is at the head of the list, how do we go further from here
#What if we sorted the OSTs differently, OSTs under same OSS are sorted, so that we can index into the OST list quickly
ost_list =ost_KB_avail.get(least_load)
print sorted(ost_list)
ost_list_sorted = sorted(ost_list)
#At the end of the mapping update OST KB-avail and repeat
#Sort OSS list
#Retrieve the OST list corresponding to min and next min and so on till the request is satisfied
#Update OST and OSS value
#At the end after all requests are processed assign the OST to the cur-req
map_ost_req = {}
flag = False
k = 0
j = oss_load_sorted[k][0]
for i in range(len(req_sorted)):
        if flag == True:
                        flag = False
                        oss_load_sorted = sorted(oss_load.items(), key=operator.itemgetter(1))
                        k = 0
                        #retrieve j such that it is equal to the head of the list
                        j = oss_load_sorted[k][0]
                        ost_list =ost_KB_avail.get(j)
                        print sorted(ost_list)
                        ost_list_sorted = sorted(ost_list)
        #when flag becomes true we break out of the loop and we can process the next request
        #But as long as the flag is false and we have not found any OST that satisfies the request we need to keep updating the ost_list
        while flag == False:
                for x in ost_list_sorted:
                        if  x >= req_sorted[i][1]:
                                flag = True
                                map_ost_req[i] = (j, ost_list.index(x)+1) #tuple of OSS_index and OST_index, assumung indices start from 1
                                #OSS value updated
                                oss_load[j] = int(oss_load.get(j)) + req_sorted[i][1]
                                print "updated: oss_load" + str(oss_load)
                                #Update OSS and OST value here and sort ost list again so that for the next request the best OST is picked
                                ost_list[ost_list.index(x)] = x - req_sorted[i][1]
                                ost_list_sorted = sorted(ost_list)
                                ost_KB_avail[j] = ost_list
                                print "updated ost_KB_avail:" + str(ost_KB_avail)
                                #Have a flag here to know if an OST has been assigned at all in this step, if it has not been choose then next lightly loaded OSS
                                break                               #key: index of request value: OST index     
                k = k+1
                if k > len(oss_load_sorted):
                        print "Req cannot be satisfied"
                        break
                j = oss_load_sorted[k][0]
                ost_list =ost_KB_avail.get(j)
                print sorted(ost_list)
                ost_list_sorted = sorted(ost_list)


print map_ost_req


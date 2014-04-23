prep = session.prepare("select count(*) from group_by_MOBILE_ID_TYPE where MOBILE_ID_TYPE = ?")
prep.consistency_level = Consist_Level

for i in range (8):
    print str(i) + " : " , session.execute(prep.bind([i]),timeout=None)

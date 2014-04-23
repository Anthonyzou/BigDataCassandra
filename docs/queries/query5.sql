prep = session.prepare("select count(*) from group_by_month where month_day = ?")
prep.consistency_level = Consist_Level
for i in range (1,32):
    print str(i) + " : " , session.execute(prep.bind([i]), timeout=None)

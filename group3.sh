#!/bin/sh
echo
echo
echo 'generate data locally : 	1'
echo 'generate data remotely : 	2 for development only'
echo '	query locally : 	3'
echo '	query remotely : 	4 for development only'
echo '		status :	5'
echo '  ssh into instance 1 :		6'
echo '	 	make zip :	7'
echo

generate(){
	nohup python -W ignore pythonClient/generate.py > generation data.txt &
}

remote_generate(){
	echo
	echo 'default group3@199.116.235.57'
	scp  -i NebulaLaunchKey.pem pythonClient/cdr_table.sql group3@199.116.235.57:/home/group3
	scp -i NebulaLaunchKey.pem pythonClient/tablestuffs.txt group3@199.116.235.57:/home/group3
	cat pythonClient/generate.py | ssh -i NebulaLaunchKey.pem group3@199.116.235.57 "python -W ignore"
}

query(){
	nohup python -W ignore pythonClient/query.py > query data.txt &
}

remote_query(){
	echo
	echo 'default group3@199.116.235.57'
	cat  pythonClient/query.py | ssh -i NebulaLaunchKey.pem group3@199.116.235.57 "python -W ignore"
}

instance1(){
	ssh -x -i NebulaLaunchKey.pem group3@199.116.235.57
}

status(){
	ssh -i NebulaLaunchKey.pem group3@199.116.235.57 "apache-cassandra-2.0.6/bin/nodetool status"
}

zip(){
	tar -cv docs query.py generate.py cdr_table.sql tablestuffs.txt NebulaLaunchKey.pem README.md | gzip -c > project.tgz
}

chmod 600 NebulaLaunchKey.pem
read input
case $input in
	1) generate;;
	2) remote_generate;;
	3) query;;
	4) remote_query;;
	5) status;;
	6) instance1;;
	7) zip;;

	*) echo "invalid";;
esac
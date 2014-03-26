#!/bin/sh
echo
echo
echo 'generate test data locally : 	1'
echo 'generate data remotely : 	2 for development only'
echo '	query locally : 	3'
echo '	query remotely : 	4 for development only'
echo '		status :	5'
echo '  ssh into instance 1 :		6'
echo '	 	make zip :	7'
echo 'generate test data optimized : 8'
echo 'generate 16TB locally : 9'
echo 'generate 16TB locally optmized : 10'

generate(){
	nohup python -W ignore generate.py 1 > misc/generation_data.txt &
}

generate_optimized(){
	nohup python -W ignore generate.py 1 true > misc/generation_data.txt &
}

generate_big(){
	nohup python -W ignore generate.py 10000 > misc/generation_data.txt &
}

generate_big_optimized(){
	nohup python -W ignore generate.py 10000 true > misc/generation_data.txt &
}

remote_generate(){
	echo
	echo 'default group3@199.116.235.57'
	scp  -i NebulaLaunchKey.pem cdr_table.sql group3@199.116.235.57:/home/group3
	scp -i NebulaLaunchKey.pem tablestuffs.txt group3@199.116.235.57:/home/group3
	cat generate.py | ssh -i NebulaLaunchKey.pem group3@199.116.235.57 "python -W ignore"
}

query(){
	nohup python -W ignore query.py > misc/query_data.txt &
}

remote_query(){
	echo
	echo 'default group3@199.116.235.57'
	cat  query.py | ssh -i NebulaLaunchKey.pem group3@199.116.235.57 "python -W ignore"
}

instance1(){
	ssh -x -i NebulaLaunchKey.pem group3@199.116.235.57
}

status(){
	ssh -i NebulaLaunchKey.pem group3@199.116.235.57 "apache-cassandra-2.0.6/bin/nodetool status"
}

zip(){
	tar -cv docs query.py misc generate.py cdr_table.sql tablestuffs.txt NebulaLaunchKey.pem README.md | gzip -c > project.tgz
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
	8) generate_optimized;;
	9) generate_big;;
	10) generate_big_optimized;;

	*) echo "invalid";;
esac
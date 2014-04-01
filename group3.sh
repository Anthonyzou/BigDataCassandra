#!/bin/sh
echo '\n\n'
echo '	generate test data locally : 		1'
echo '	generate data remotely : 		2 for development only'
echo '	query locally : 			3'
echo '	query remotely : 			4 for development only'
echo '	status :				5'
echo '	ssh into instance 1 :			6'
echo '	make zip :				7'
echo '	generate 16TB locally :			8'
echo '	setup this machine :			9'
echo '\n\n'

generate(){
	echo '		enter days to generate!'
	read amount
	echo '		ENTER SEED'
	read seed 
	echo '		ENTER GENERATION DATA FILE'
	read file
	if[[-x "$seed"]]; then
		nohup python -W ignore generate.py "$amount" > misc/"$file" &
	else
		nohup python -W ignore generate.py "$amount" "$seed" > misc/"$file" &
	fi
	echo 'data goes to misc/"$file"'
}
generate_big(){
	nohup python -W ignore generate.py 10000 > misc/generation_data.txt &
	echo 'data goes to misc/generation_data.txt'
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
	echo 'data goes to misc/query_data.txt'
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

setup(){
	curl http://www.apache.org/dyn/closer.cgi?path=/cassandra/2.0.6/apache-cassandra-2.0.6-bin.tar.gz | tar xf C $HOME
	export PATH=$PATH:$HOME/apache-cassandra-2.0.6/bin
	echo '	ENTER THIS MACHINES IP'
	read input
	sed "s/10.0.0.31/$input/g" misc/cassandra.yaml >> $HOME/apache-cassandra-2.0.6/conf/cassandra.yaml
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
	8) generate_big;;
	9) setup;;

	*) echo "exiting";;
esac
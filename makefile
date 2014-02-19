java:
	ant
	scp -i NebulaLaunchKey.pem javaClient/bin/client.jar group3@199.116.235.57:/home/group3
python:
	scp -i NebulaLaunchKey.pem pythonClient/client.py group3@199.116.235.57:/home/group3
ruby:
	scp -i NebulaLaunchKey.pem rubyClient/client.rb group3@199.116.235.57:/home/group3

instance1:
	ssh -x -i NebulaLaunchKey.pem group3@199.116.235.57

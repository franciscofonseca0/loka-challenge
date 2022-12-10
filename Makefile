build_images:
    docker-compose build 

run_images:
	docker-compose up airflow-init
	docker-compose up

aws_deploy:
	docker build . -f aws_deploy.Dockerfile -t aws_deploy
	docker run --env-file=.env aws_deploy bootstrap
	docker run --env-file=.env aws_deploy deploy
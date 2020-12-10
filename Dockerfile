FROM ubuntu

RUN apt update
RUN apt upgrade -y
RUN apt install -y gcc make libmysqlclient-dev cpanminus libdbi-perl
RUN cpanm install Devel::CheckLib
RUN cpanm install Test::Deep

## Add the wait script to the image
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.7.3/wait /wait
RUN chmod +x /wait

WORKDIR /driver
COPY . .

RUN perl ./Makefile.PL --testuser=root --testhost=mysqldb --testport=3306 --testdb=test 

CMD /wait && make && make test


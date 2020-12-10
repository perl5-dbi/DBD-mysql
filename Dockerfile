FROM ubuntu

#ENV http_proxy=http://www-proxy.us.oracle.com:80
#ENV https_proxy=http://www-proxy.us.oracle.com:80

RUN apt update
RUN apt upgrade -y
RUN apt install -y gcc make libmysqlclient-dev cpanminus libdbi-perl
RUN cpanm install Devel::CheckLib
RUN cpanm install Test::Deep

ENV http_proxy=
ENV https_proxy=

## Add the wait script to the image
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.7.3/wait /wait
RUN chmod +x /wait

WORKDIR /driver
COPY . .

RUN perl ./Makefile.PL --testuser=root --testhost=mysqldb --testport=3306 --testdb=test 

CMD /wait && make && make test


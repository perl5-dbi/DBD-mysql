FROM perl:5.42

# Add MySQL APT Repository
RUN apt-get update
RUN apt-get install -y lsb-release debconf-utils cpanminus
ADD https://dev.mysql.com/get/mysql-apt-config_0.8.36-1_all.deb .
RUN DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_0.8.36-1_all.deb

RUN apt-get update
RUN apt-get install -y libmysqlclient-dev
RUN cpanm DBD::mysql

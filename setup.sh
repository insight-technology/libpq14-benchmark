#!/bin/sh

LIBPQ_VER="14.1"

curl -LO "https://ftp.postgresql.org/pub/source/v${LIBPQ_VER}/postgresql-${LIBPQ_VER}.tar.gz"

tar xf "postgresql-${LIBPQ_VER}.tar.gz"

rm -r ./libpq
mkdir ./libpq

cd "postgresql-${LIBPQ_VER}" && ./configure --prefix=$(pwd)/../libpq --with-openssl && make && make install

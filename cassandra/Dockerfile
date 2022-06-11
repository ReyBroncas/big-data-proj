FROM cassandra:3.11

COPY ./init.sh /init.sh
ENTRYPOINT ["/init.sh"]
CMD ["cassandra", "-f"]

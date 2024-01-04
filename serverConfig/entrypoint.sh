#!/bin/bash
supervisord -c /etc/supervisord.conf

flask sync-protein-database
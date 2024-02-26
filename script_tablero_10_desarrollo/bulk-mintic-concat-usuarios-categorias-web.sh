#!/bin/bash
cd /logstash1/anaconda3/;
source bin/activate;
for i in {1..800}
do
    echo "ejecuta $i";
    python /home/desarrollo/gh-mintic-backend/scripts_test/mintic-concat-usuarios-categorias-web.py >> /home/desarrollo/gh-mintic-backend/scripts_test/logs/mintic-concat-usuarios-categorias-web.log;
done
conda deactivate

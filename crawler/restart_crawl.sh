source /mnt/tianwen-course/tianwen/home/wenyh/.zshrc
heritrix -a admin:admin -j "/mnt/tianwen-course/tianwen/home/wenyh/crawler/heritrix_astro_crawler/jobs" -p 8443
heri build astro_crawler
heri start astro_crawler
heri continue astro_crawler
#cd /mnt/tianwen-course/tianwen/home/zhangjianxing/spider/jobs

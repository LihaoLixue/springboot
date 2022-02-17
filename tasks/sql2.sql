update cron set flage=( case (select (select date_format(now(),'%Y%m%d'))=( select date_format(now(),'%Y%m%d'))) when 1 then '1' when 0 then '0' end     ) where main_id=0;

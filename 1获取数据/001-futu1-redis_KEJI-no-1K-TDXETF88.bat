@echo off
:loop

cd D:\MOO-ETF88
python 001-futu1-redis_KEJI-no-1K-TDXETF88.py
:: ½üËÆµÈ´ý300ºÁÃë
ping localhost -n 1 -w 100 > nul
goto loop
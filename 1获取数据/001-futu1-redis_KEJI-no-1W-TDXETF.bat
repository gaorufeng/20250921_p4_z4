@echo off
:loop

cd E:\FUFU
python 001-futu1-redis_KEJI-no-1W-TDXETF.py
:: ���Ƶȴ�300����
ping localhost -n 1 -w 100 > nul
goto loop
@echo off 
mode con cols=100 lines=30 
D:  
cd %cd%

python getKLineDayCNY.py 
python getKLineDayUSD.py
python getKLineDayEXC.py

python getKLineHourCNY.py 
python getKLineHourUSD.py
python getKLineHourEXC.py

python getKLineMinCNY.py 
python getKLineMinUSD.py
python getKLineMinEXC.py

pause
exit

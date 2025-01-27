#/bin/bash

SHE_JSON=she_measurements_listfile.json
SIM_JSON=tu_output_listfile.json

echo -n "[" > $SHE_JSON
echo -n "[" > $SIM_JSON

FIRST=1

for i in {0..100000}
do
	SHE_FN=`grep -l \<ObservationId\>$i\</ObservationId\> *.xml`
	if [ $? -eq 1 ]; then
		continue
    fi
	SIM_FN=`grep -l \<EuclidPointingId\>$i\</EuclidPointingId\> *.xml`
	if [ $? -eq 1 ]; then
		continue
    fi
   
    if [ $FIRST -eq 1 ]; then
    	FIRST=0
    else
   		echo -n , >> $SHE_JSON
		echo -n , >> $SIM_JSON 	
	fi

	echo -n \" >> $SHE_JSON
	echo -n \" >> $SIM_JSON
	
	echo -n $SHE_FN >> $SHE_JSON
	echo -n $SIM_FN >> $SIM_JSON	

	echo -n \" >> $SHE_JSON
	echo -n \" >> $SIM_JSON
done

echo -n "]" >> $SHE_JSON
echo -n ] >> $SIM_JSON
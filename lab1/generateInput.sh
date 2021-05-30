#!/bin/bash
COUNTER=0
if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi

rm -rf input
mkdir input

for i in {1..200}
	do 
                COUNTER=$(($COUNTER + 1))
                COUNTER_OST=$(($COUNTER %10))
                ID=$((1 + RANDOM % 2))
		CURRENTTIME=`date +"%s"`
                RAND=$((1 + RANDOM % 150))
                CURRENTTIME=$(($CURRENTTIME +$((1 + RANDOM % 4))))
		VALUE=$((1+ RANDOM %300))
		OST=$(($VALUE %5))
		if [ $COUNTER_OST -eq 0 ]
                then
                      echo ${COUNTER} >> input/$1.1
                elif [ $OST -eq 0 ] 
		then
		      echo ${ID}, ${CURRENTTIME}, ${VALUE} >> input/$1.1
	        else 
		      VALUE=$(($VALUE +5 - $OST))
		      echo ${ID}, ${CURRENTTIME}, ${VALUE} >> input/$1.1
		fi
	done


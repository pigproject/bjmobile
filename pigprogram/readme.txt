1. hadoop jar bjmobile.jar com.bjmobile.BjSelectField /tmp/record/805_Part3.csv /tmp/record/805_Part3_out09 
   hadoop jar bjmobile.jar com.bjmobile.BjSelectField /tmp/record/csmo-csmt-lu-request-2.txt /tmp/record/BjSelectField_out01
2. pig -param vlr_path=/tmp/record/VLR.dat -param  output=/tmp/record/record_out/pig02result pig02.pig
3. pig -param vlr_path=/tmp/record/VLR.dat -param  rtt_path=/tmp/record/RTT.dat -param output=/tmp/record/record_out/pig03result pig03.pig
4. pig -param vlr_path=/tmp/record/VLR.dat -param  output=/tmp/record/record_out/pig04result pig04.pig
5. pig -param vlr_path=/tmp/record/VLR.dat -param  rtt_path=/tmp/record/RTT.dat -param output=/tmp/record/record_out/pig05result pig05.pig

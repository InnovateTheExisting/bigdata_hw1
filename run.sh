cd out/artifacts/HW1

hadoop jar HW1.jar CommonFriends ../../../soc-LiveJournal1Adj.txt CommonFriends_output1 CommonFriends_output2
hadoop jar HW1.jar Top10 CommonFriends_output1/part-r-00000 Top10_output
hadoop jar HW1.jar CommonFriendsStates CommonFriends_output1/part-r-00000 CommonFriendsStates_output ../../../userdata.txt 0 1
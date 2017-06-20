import commands
for i in range(1, 5):
    print " screen -dmS chat_message_part_" + str(i) + ""
    commands.getoutput("screen -dmS chat_message_part_" + str(i) + " bash")
    commands.getoutput("screen -S chat_message_part_" + str(
        i) + " -p 0 -X exec sudo python chat_data_migration_v2_to_v3.py " + str(i) + "")

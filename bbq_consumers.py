import pika
import sys
from collections import deque

# limited to 5 items (the 5 most recent readings)
smoker_deque = deque(maxlen=5)
# limited to 20 items (the 20 most recent readings)
food_A_deque = deque(maxlen=20)
# limited to 20 items (the 20 most recent readings)
food_B_deque = deque(maxlen=20)

# define a callback function to be called when a message is received
def smoker_callback(ch, method, properties, body):
    """ Define behavior on getting a message about the smoker temperature."""
    
    # add the temp to the deque
    smoker_deque.append(body.decode())
    #seperate the temp from the dat/time by using split
    message = body.decode().split(",")
    #assign the temp to a variable making it a float
    smoker_temp = float("0.0" if message[1] == 'Channel1' or message[1]== '' else message[1])
    
    #check to see that the deque has 5 items before analyzing
    if len(smoker_deque) == 5:
        # read rightmost item in deque and subtract from leftmost item in deque
        #assign difference to a variable as a float rounded to 2
        Smoker_temp_check = round(float(smoker_deque[-1]-smoker_deque[0]),2)
        #if the temp has changed by 15 degress then an alert is sent
        if Smoker_temp_check < -15:
            print("Current smoker temp is:", str(smoker_temp[0]),";", "Smoker temp change in last 2.5 minutes is:", Smoker_temp_check)
            print("smoker alert!")
        #Show work in progress, letting the user know the changes
        else:
            print("Current smoker temp is:", str(smoker_temp[0]),";", "Smoker temp change in last 2.5 minutes is:", Smoker_temp_check)
    else:
        #if the deque has less than 5 items the current temp is printed
        print("Current smoker temp is:", str(smoker_temp[0]))
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_A_callback(ch, method, properties, body):
    """ Define behavior on getting a message about FoodA temperature."""
    
     # add the temp to the deque
    food_A_deque.append(body.decode())
    #seperate the temp from the dat/time by using split
    message = body.decode().split(",")    
    #assign the temp to a variable making it a float
    food_A_temp = float("0.0" if message[1] == 'Channel2' or message[1]== '' else message[1])
   
    #check to see that the deque has 5 items before analyzing
    if len(food_A_deque) == 20:
        # read rightmost item in deque and subtract from leftmost item in deque
        #assign difference to a variable as a float rounded to 2
        food_A_temp_check = round(float(food_A_deque[-1]-food_A_deque[0]),2)
        #if the temp has changed less than 1 degree then an alert is sent
        if food_A_temp_check < 1:
            print("Current Food A temp is:", str(food_A_temp[0]),";", "Food A temp change in last 10 minutes is:", food_A_temp_check)
            print("Food stall alert! Food A temperature changed by 1 degree or less in 10 minutes!")
        #Show work in progress, letting the user know the changes
        else:
            print("Current Food A temp is:", str(food_A_temp[0]),";","Food A temp change in last 10 minutes is:", food_A_temp_check)
    else:
        #if the deque has less than 20 items the current temp is printed
        print("Current Food A temp is:", str(food_A_temp[0]))
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_B_callback(ch, method, properties, body):
    """ Define behavior on getting a message about FoodB temperature."""
     
    food_B_deque.append(body.decode())
    #seperate the temp from the dat/time by using split
    message = body.decode().split(",")    
    #assign the temp to a variable making it a float
    food_B_temp = float("0.0" if message[1] == 'Channel3' or message[1]== '' else message[1])
    # add the temp to the deque
    
    #check to see that the deque has 5 items before analyzing
    if len(food_B_deque) == 20:
        # read rightmost item in deque and subtract from leftmost item in deque
        #assign difference to a variable as a float rounded to 2
        food_B_temp_check = round(float(food_B_deque[-1]-food_B_deque[0]),2)
        #if the temp has changed less than 1 degree then an alert is sent
        if food_B_temp_check < 1:
            print("Current Food B temp is:", str(food_B_temp[0]),";", "Food B temp change in last 10 minutes is:", food_B_temp_check)
            print("food stall alert! Food B temperature changed by 1 degree or less in 10 minutesB!")
        #Show work in progress, letting the user know the changes
        else:
            print("Current Food B temp is:",str(food_B_temp[0]),";","Food B temp change in last 10 minutes is:", food_B_temp_check)
    else:
        #if the deque has less than 20 items the current temp is printed
        print("Current Food B temp is:", str(food_B_temp[0]))
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str, queue1: str, queue2: str, queue3: str):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=queue1, durable=True)
        channel.queue_declare(queue=queue2, durable=True)
        channel.queue_declare(queue=queue3, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=queue1, auto_ack = False, on_message_callback=smoker_callback)
        channel.basic_consume(queue=queue2, auto_ack = False, on_message_callback=food_A_callback)
        channel.basic_consume(queue=queue3, auto_ack = False, on_message_callback=food_B_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        # delete the queues when complete so unprocessed messages are cleared
        channel.queue_delete(queue1)
        channel.queue_delete(queue2)
        channel.queue_delete(queue3)
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main('localhost','01-smoker','02-food-A','02-food-B')
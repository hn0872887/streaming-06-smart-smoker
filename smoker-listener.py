"""This program listens for work messages contiously.
    
    Name: Hanh Nguyen
    Date: 2/21/23
    We want know if (Condition To monitor):
    The smoker temperature decreases by more than 15 degrees F in 2.5 minutes (smoker alert!)
    Any food temperature changes less than 1 degree F in 10 minutes (food stall!)
    Three consumer processes, each one monitoring one of the temperature streams. 
    Perform calculations to determine if a significant event has occurred.
    Time Windows:
    Smoker time window is 2.5 minutes
    Food time window is 10 minutes
    Deque Max Length:
    At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)
    At one reading every 1/2 minute, the food deque max length is 20 (10 min * 1 reading/0.5 min) 
    Three listening queues: 01-smoker, 02-food-A, 02-food-B
    Three listening callback functions: smoker_callback, fooda_callback, foodb_callback
    !!!!!Start the emitter first or it closes out the listener since the queue delete is part of the script!!!!!
"""
import pika
import sys
from collections import deque

smoker_deque = deque(maxlen = 5)
smoker_alert = "ALERT! Smoker temperature is decreasing more than 15 degrees in 2.5 minutes"


# define a callback function to be called when a message is received
def smoker_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    #splitting the smoker data to isolate temp
    smoker_message =  body.decode().split(",")
    # creating a temp variable
    temp = [0]
    #changing the temp string to a float
    temp[0] = float(smoker_message[1])
    #placing the temp data in the right side of the deque
    smoker_deque.append(temp[0])
    
    smoker_temp_change = smoker_deque[0]-smoker_deque[-1]
    #creating the alert
    if len(smoker_deque) == 5:
        
        if smoker_temp_change > 15:
            print(smoker_alert)
            print(f" Smoker temp change in last 2.5 minutes is: {smoker_temp_change}")
    # decode the binary message body to a string
    print(f"[x] Received.  Smoker temp is {smoker_message}. Smoker temp change in last 2.5 minutes is: {smoker_temp_change}")
    # decode the binary message body to a string
    # when done with task, tell the user
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue"):
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
        channel.queue_declare(queue=qn, durable=True)

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
        channel.basic_consume( queue=qn, auto_ack=False, on_message_callback=smoker_callback)

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
        channel.queue_delete(qn)
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "01-smoker")
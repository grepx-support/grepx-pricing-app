"""
Prefect Learning Example 01: Simple Flow
========================================
Basic example demonstrating a simple Prefect flow
"""
from prefect import flow, task
from datetime import datetime


@task(name="get_current_time")
def get_current_time():
    """A simple task that returns the current time"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Current time is: {current_time}")
    return current_time


@task(name="greet_user")
def greet_user(name: str, time: str):
    """A simple task that greets a user"""
    message = f"Hello {name}! Current time is {time}"
    print(message)
    return message


@flow(name="Simple Greeting Flow")
def simple_greeting_flow(user_name: str = "User"):
    """
    A simple flow that demonstrates:
    - Defining tasks
    - Calling tasks within a flow
    - Passing data between tasks
    """
    print("Starting simple greeting flow...")
    
    # Call the first task
    current_time = get_current_time()
    
    # Call the second task with parameters
    greeting = greet_user(user_name, current_time)
    
    print(f"Flow completed with greeting: {greeting}")
    return greeting


if __name__ == "__main__":
    # Run the flow
    result = simple_greeting_flow("Alice")
    print(f"Final result: {result}")
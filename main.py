import sys

def run_producer():
    from producer import produce
    produce(rate=20, total=100)  # Adjust as needed

def run_consumer():
    from consumer import consume
    consume()

def run_dashboard():
    import os
    os.system("streamlit run dashboard.py")  # Requires streamlit installed

def main():
    print("Select an option:")
    print("1. Run Producer")
    print("2. Run Consumer")
    print("3. Run Dashboard")

    choice = input("Enter 1, 2 or 3: ")

    if choice == "1":
        run_producer()
    elif choice == "2":
        run_consumer()
    elif choice == "3":
        run_dashboard()
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()

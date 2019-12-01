#! /usr/bin/env python
import time
import threading
import logging
import tkinter as tk
import tkinter.scrolledtext as ScrolledText


class TextHandler(logging.Handler):
    # This class allows you to log to a Tkinter Text or ScrolledText widget
    # Adapted from https://stackoverflow.com/questions/13318742/python-logging-to-tkinter-text-widget

    def __init__(self, text):
        # run the regular Handler __init__
        logging.Handler.__init__(self)
        # Store a reference to the Text it will log to
        self.text = text

    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text.configure(state='normal')
            self.text.insert(tk.END, msg + '\n')
            self.text.configure(state='disabled')
            # Autoscroll to the bottom
            self.text.yview(tk.END)
        # This is necessary because we can't modify the Text from other threads
        self.text.after(0, append)

class ScraperGUI(tk.Frame):
    start_year_txt = "2019"
    start_month_txt = "January"

    # This class defines the graphical user interface 
    
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.root = parent
        self.build_gui(parent=parent)
        
    def build_gui(self, parent):                    
        # Build GUI
        self.root.title('ND Gas/Oil Data Scraper')

        self.label = tk.Label(parent, text="Choose Scrape Start Date")
        self.label.grid(row=0, column=0, columnspan=4)

        self.greet_button = tk.Button(parent, text="Scrape", command=self.greet)
        self.greet_button.grid(row=1, column=0, columnspan=4)

        self.close_button = tk.Button(parent, text="Close", command=parent.quit)
        self.close_button.grid(row=2, column=0, columnspan=4)

        self.root.option_add('*tearOff', 'FALSE')
        self.grid(column=0, row=4, sticky='ew')
        self.grid_columnconfigure(3, weight=1, uniform='a')
        self.grid_columnconfigure(4, weight=1, uniform='a')
        self.grid_columnconfigure(5, weight=1, uniform='a')
        self.grid_columnconfigure(6, weight=1, uniform='a')
        
        
        # Add text widget to display logging info
        st = ScrolledText.ScrolledText(self, state='disabled')
        st.configure(font='TkFixedFont')
        st.grid(column=0, row=4, sticky='w', columnspan=4)

        # Create textLogger
        text_handler = TextHandler(st)
        
        # Logging configuration
        logging.basicConfig(level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s')        
        
        # Add the handler to logger
        logger = logging.getLogger()        
        logger.addHandler(text_handler)

    def greet(self):
        logging.info("Scraping from {}-{}".format(self.start_month_txt, self.start_year_txt))
              
def worker():
    # Skeleton worker function, runs in separate thread (see below)   
    # Report time / date at 2-second intervals
    time.sleep(2)
    timeStr = time.asctime()
    msg = 'Current time: ' + timeStr
    logging.info(msg) 
        
def main():
    
    root = tk.Tk()
    ScraperGUI(root)
    
    t1 = threading.Thread(target=worker, args=[])
    t1.start()
        
    root.mainloop()
    t1.join()
    
main()
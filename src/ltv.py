"""
 This code was built using canopy as tool with PYTHON 3.5. 

 Calculate the LTV based on the events data - e

 Functions : 
   INGEST :                 Takes the event data e and build dataframes for customer , site visits and orders. 
                            Image data could also have been used to loaded into dataframe for future usage.
 
   TopXSimpleLTVCustomers :
                            Reads the Dataframes built in the ingest funstion and transforms the data for 
                            calculating LTV. Calculates top customers by LTV.
                            The new dataframes could be saved as csv for future reference.
 
 OUTPUT : 
                            The code was run in python 3.5.This code will not run in version lower then 3.5. 
                            OUTPUT for this is generated as :-
                            
                                        amount  count  weeks  life_span      LTV
                            customer_id                                             
                            C            14.040000    2.5     52         10  18252.0
                            V             9.506667    1.5     52         10   7415.2

"""

import pandas as pd
import json
import numpy
from datetime import datetime , timedelta

WEEKS_IN_YEAR = int(52)
CUSTOMER_LIFE_SPAN = int(10)

# TODO : This could be an input file or any data strcture coming in as an input.
e_str = [{"type": "CUSTOMER", "verb": "NEW", "key": "Y", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"},
{"type": "CUSTOMER", "verb": "UPDATE", "key": "Y", "event_time": "2017-01-06T13:48:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AS"},
{"type": "CUSTOMER", "verb": "NEW", "key": "C", "event_time": "2017-01-06T12:48:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AC"},
{"type": "CUSTOMER", "verb": "NEW", "key": "V", "event_time": "2017-01-06T12:48:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AC"},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S1", "event_time": "2017-02-01T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S2", "event_time": "2017-02-03T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S3", "event_time": "2017-02-04T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S4", "event_time": "2017-02-05T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S5", "event_time": "2017-02-06T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S5", "event_time": "2017-02-07T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S5", "event_time": "2017-02-10T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S5", "event_time": "2017-02-14T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S5", "event_time": "2017-02-16T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S5", "event_time": "2017-02-20T12:45:52.041Z", "customer_id": "C", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-02-05T12:45:52.041Z", "customer_id": "Y", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-05-05T12:45:52.041Z", "customer_id": "Y", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-09-05T12:45:52.041Z", "customer_id": "Y", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-02-05T12:45:52.041Z", "customer_id": "V", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-02-06T12:45:52.041Z", "customer_id": "V", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-02-13T12:45:52.041Z", "customer_id": "V", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-02-15T12:45:52.041Z", "customer_id": "V", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-03-21T12:45:52.041Z", "customer_id": "V", "tags": [{"some key": "some value"}]},
{"type": "SITE_VISIT", "verb": "NEW", "key": "S6", "event_time": "2017-03-23T12:45:52.041Z", "customer_id": "V", "tags": [{"some key": "some value"}]},
{"type": "IMAGE", "verb": "UPLOAD", "key": "d8ede43b1d9f", "event_time": "2017-01-06T12:47:12.344Z", "customer_id": "96f55c7d8f42", "camera_make": "Canon", "camera_model": "EOS 80D"},
{"type": "ORDER", "verb": "NEW", "key": "O1", "event_time": "2017-02-01T12:55:55.555Z", "customer_id": "C", "total_amount": "10.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O2", "event_time": "2017-02-03T12:55:55.555Z", "customer_id": "C", "total_amount": "8.34 USD"},
{"type": "ORDER", "verb": "UPDATE", "key": "O2", "event_time": "2017-02-04T12:55:55.555Z", "customer_id": "C", "total_amount": "12.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O3", "event_time": "2017-02-04T12:55:55.555Z", "customer_id": "C", "total_amount": "12.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O4", "event_time": "2017-02-05T12:55:55.555Z", "customer_id": "C", "total_amount": "9.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O5", "event_time": "2017-02-06T12:55:55.555Z", "customer_id": "C", "total_amount": "11.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O6", "event_time": "2017-02-07T12:55:55.555Z", "customer_id": "C", "total_amount": "18.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O7", "event_time": "2017-02-10T12:55:55.555Z", "customer_id": "C", "total_amount": "8.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O8", "event_time": "2017-02-14T12:55:55.555Z", "customer_id": "C", "total_amount": "20.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O9", "event_time": "2017-02-16T12:55:55.555Z", "customer_id": "C", "total_amount": "22.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O10", "event_time": "2017-02-20T12:55:55.555Z", "customer_id": "C", "total_amount": "15.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O11", "event_time": "2017-02-05T12:55:55.555Z", "customer_id": "Y", "total_amount": "5.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O12", "event_time": "2017-05-05T12:55:55.555Z", "customer_id": "Y", "total_amount": "5.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O13", "event_time": "2017-09-05T12:55:55.555Z", "customer_id": "Y", "total_amount": "5.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O14", "event_time": "2017-02-05T12:55:55.555Z", "customer_id": "V", "total_amount": "8.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O15", "event_time": "2017-02-06T12:55:55.555Z", "customer_id": "V", "total_amount": "10.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O16", "event_time": "2017-02-13T12:55:55.555Z", "customer_id": "V", "total_amount": "9.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O17", "event_time": "2017-02-15T12:55:55.555Z", "customer_id": "V", "total_amount": "7.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O18", "event_time": "2017-02-21T12:55:55.555Z", "customer_id": "V", "total_amount": "11.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "O19", "event_time": "2017-02-23T12:55:55.555Z", "customer_id": "V", "total_amount": "10.34 USD"}
]



def Ingest(e):
    # Read data from e and store it into a Dataframe    
    df = pd.DataFrame.from_dict(e_str, orient='columns')
    
    # Creates a customer dataframe
    df_customer = df.loc[df['type'].isin(['CUSTOMER']),['key','verb','event_time','last_name','adr_city','adr_state']]
    df_customer = df_customer.rename(columns={'key': 'customer_id'})

    # Creates a Site Visit dataframe
    df_site_visit = df.loc[df['type'].isin(['SITE_VISIT']),['key','verb','event_time','customer_id','tags']]
    df_site_visit = df_site_visit.rename(columns={'key': 'page_id'})
    
    # Creates a Order dataframe
    df_order=df.loc[df['type'].isin(['ORDER']),['key','verb','event_time','customer_id','total_amount']]
    df_order = df_order.rename(columns={'key': 'order_id'})
    
    # Creates customer dataframe for new records
    df_customer_new = df_customer.loc[df_customer['verb']=='NEW']
    # Creates customer dataframe for update records
    df_customer_update = df_customer.loc[df_customer['verb']=='UPDATE']
    
    df_customer_new.set_index('customer_id', inplace=True)
    df_customer_update.set_index('customer_id', inplace=True)
    
    # Updates customer data for the update events
    df_customer_new.update(df_customer_update)
    
    # Slpit the Order amount and currency
    df_order['amount'], df_order['curency'] = df_order['total_amount'].str.split(' ', 1).str
    df_order['amount'] = df_order['amount'].astype(float)

    # Creates order dataframe for new records
    df_order_new = df_order.loc[df_order['verb']=='NEW']

    # Creates order dataframe for update records
    df_order_update = df_order.loc[df_order['verb']=='UPDATE']
    
    df_order_new.set_index('order_id', inplace=True)
    df_order_update.set_index('order_id', inplace=True)
    
    # Updates Order data for the update events
    df_order_new.update(df_order_update)
    return (df_customer_new,df_order_new,df_site_visit)

def TopXSimpleLTVCustomers(x, df_customer_new,df_order_new,df_site_visit):
    
    # Get all Orders for customer - join customer and order data on customer_id
    df_customer_order = df_customer_new.join(df_order_new.set_index('customer_id'),how='inner',lsuffix='_cust', rsuffix='_ord')
    df_customer_order['amount'].fillna(0, inplace=True)
    df_customer_order['event_time_ord'] = pd.to_datetime(df_customer_order['event_time_ord'])
    
    # Calculate the week_start date for each Order placed by customer    
    df_customer_order['week_start'] = df_customer_order['event_time_ord'].dt.to_period('W').apply(lambda r: r.start_time)
    df_customer_order.drop(['verb_cust','verb_ord','event_time_cust','last_name','adr_city','adr_state','total_amount'], axis=1, inplace=True)
    
    # Get all site visit for customer - join customer and site visit based on customer_id
    df_customer_site_visit = df_customer_new.join(df_site_visit.set_index('customer_id'),how='inner',lsuffix='_cust', rsuffix='_site')
    df_customer_site_visit['event_time_site']=pd.to_datetime(df_customer_site_visit['event_time_site'])
    
    # Calculate the week_start date for each Site Visit by customer    
    df_customer_site_visit['week_start'] = df_customer_site_visit['event_time_site'].dt.to_period('W').apply(lambda r: r.start_time)
    df_customer_site_visit.drop(['verb_cust','verb_site','event_time_cust','tags','last_name','adr_city','adr_state','page_id'], axis=1, inplace=True)
    
    # Get average Order amount(expenditure) for each customer
    # TODO : Write to csv for future reference - Average customer expenditure per visit
    df_avg_customer_order = df_customer_order.groupby([df_customer_order.index.get_level_values(0)]).mean()
    
    # Get count of visits on weekly basis and then calculate the average site visit for each customer
    df_customer_site_visit_weekly = df_customer_site_visit.groupby([df_customer_site_visit.index.get_level_values(0),'week_start']).count()
    df_customer_site_visit_weekly = df_customer_site_visit_weekly.rename(columns={'event_time_site': 'count'})
    
    # TODO : Write to csv for future reference - Average customer visits per week
    df_avg_weekly_customer_visits = df_customer_site_visit_weekly.groupby(['customer_id']).mean()
    
    # Combine average order/expenditure amount and number of site visit 
    df_customer_avg_order_visits = df_avg_customer_order.join(df_avg_weekly_customer_visits,how='inner',lsuffix='_cust', rsuffix='_site')
    df_customer_avg_order_visits['weeks'] = WEEKS_IN_YEAR
    df_customer_avg_order_visits['life_span'] = CUSTOMER_LIFE_SPAN
    
    # Calculate LTV based on formula -
    # lifespan(10) * (number of weeks(52) * (avg customer expenditure per visit * avg number of weekly visit ))
    df_customer_avg_order_visits['LTV'] = df_customer_avg_order_visits['life_span']*(df_customer_avg_order_visits['weeks']*(df_customer_avg_order_visits['amount']*df_customer_avg_order_visits['count']))
    
    return df_customer_avg_order_visits.nlargest(x, 'LTV')


def main():
    # Ingest event data
    (df_customer_new, df_order_new, df_site_visit) = Ingest(e_str)
    
    x = 2
    # Get top X customer LTV
    top_x_customer = TopXSimpleLTVCustomers(x, df_customer_new, df_order_new, df_site_visit)
    print(top_x_customer)
    
    # TODO : Can save dataframe df_customer_new, df_order_new, df_site_visit to csv in the output folder for future usage    


if __name__ == '__main__':
  main()

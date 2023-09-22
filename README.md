# customer-reward-calculation
Calculate customer rewards point

# This repo having implementation of below problem.
•	A customer receives 2 points for every dollar spent over $100 in each transaction, plus 1 point for every dollar spent over $50 in each transaction
(e.g. a $120 purchase = 2x$20 + 1x$50 = 90 points).
•	Given a record of every transaction during a three month period, calculate the reward points earned for each customer per month and total.


# POST use for  create data in Map/ DB.
http://localhost:8080/api/addDetail


#JSON TestDATA
[
{
        "name":"customer1",
        "amount":130.0,
        "date": "2023-06-05"
    },
    {
        "name":"customer2",
        "amount":160.0,
        "date": "2023-07-25"
    },
    {
        "name":"customer3",
        "amount":90.0,
        "date": "2023-08-15"
    }
]

# GET use for fetch data from storags.
http://localhost:8080/api/reward-points
 


# Processing complex Data :

#### Files in this directory contain code for converting complex json to simpler csv and vice-versa 

### Input file :
- Type : JSON
- Example : [event.json](https://github.com/dubeyabhi07/big-data-spark/blob/master/src/main/resources/EVENT/event.json)
- Schema : 
```json

event_data:array
  element:struct
    event_id:string
    reserved:struct
      confirmed:array
        element:struct
          address:string
          city:string
          slots:long
      waitlist:array
         element:struct
           city:string
           slots:long
    schedule:struct
      <city_1>:array
        element:struct
          cost:string
          date:string
      <city_2>:array
        element:struct
          cost:string
          date:string
          .
          .
          .
          .

``` 

### Output file :
- Format  : csv
- Schema :
  - reservation.csv :
  ```json
  event_id:string
  confirmed_city:string
  details:array
    element:struct
      address:string
      slots:long
  total_confirmed_slots:long
  waitlist_city:string
  total_waitlist_slots:long
  ```
  - schedule.csv : 
  ```json
  event_id:string
  city:string
  cost:string
  date:string
  ```



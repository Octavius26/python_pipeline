
# Piplines
This module aims to ease the creation of processing tools.

# Key features 

Here is a list of somes keyfeatures available with a pipeline object :

## One line execution (from input to output)
```python
output = pipe.execute(input_data)
```
## Acces to every steps results 
```python
step_result = pipe.get_data(data_name)
```

## Showing steps timings
```python
timing_df = pipe.give_timings()
```

## Allow many output / inputs
- Every kind of inputs / outputs are avalaible
- Tuple output can be split to be associated with diffrents ouptut names
- Output datas can be unused
- Ouput data can be overwritten (not recommended)

- Multiple pipe inputs are available, but need to be specify during the creation of the pipe.  
  And then a dictionary is required for the .execute() function
```python
pipe = Pipeline(l_input_names=['name_1','name_2'])
pipe.execute(dict_data={'name_1':1,'name_2':2})
```



# Download
This module is not availaible on pip. You will need to add manually to your porject.
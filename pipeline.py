from __future__ import annotations

import numpy as np
import time
import pandas as pd

class C_step:
	process_time = 0
	nb_run = 0

	def __init__(	self,
		  			func,
					name : str,
					l_input_name : list,
					l_output_name : list,
					func_kwargs : dict):
		"""
		Args
		----
		- `func` : function
		- `name` : str
		- `l_input_name` : list
		- `l_output_name` : list
		- `func_kwargs` : dict
			- kwargs to use with `func`
		"""
		# TODO allow to create steps which are sub_pipelines
		self.func = func
		self.name = name
		self.l_input_name = l_input_name
		self.l_output_name = l_output_name
		self.func_kwargs = func_kwargs

	def run(self,l_arg):
		t_start = time.perf_counter()
		result = self.func(*l_arg,**self.func_kwargs)
		t_end = time.perf_counter()
		self.process_time += t_end - t_start
		self.nb_run += 1
		return result

	def reset(self):
		self.process_time = 0
		self.nb_run = 0


class Pipeline:
	def __init__(self,allow_overwrite:bool=False,l_input_names:list[str]=None):
		"""
		Args
		----
		- `allow_overwrite` : bool (default = False)
			- allow the user to overwrite a previous output (using the same output_name)
		- `l_input_names` : list[str] (default = None)
			- used to specify the input(s) name(s)
			- None => ['input']
		"""
		
		if l_input_names is None : l_input_names = ['input']
		self.l_steps : list[C_step] = []
		self.allow_overwrite = allow_overwrite
		self.l_input_names = l_input_names

		# --- Pre-define inputs to avoid missing input flag during step adding ---
		self.data = {}
		for input_name in l_input_names : 
			self.data[input_name] = None

	def add_step(	self, 
					l_input_name : list[str],
					func=None, 
					func_kwargs : dict = None,
		  			name:str=None,
					l_output_name : list[str]=None): 
		"""
		Add a processing step to the pipeline.
		
		Args
		----
		`l_inputs_names` : list[str]  
		
		`func` : function (default = None)
		
		`func_kwargs` : dict
			Additional parameters to use with func
		
		`name` : str (default = None)
			if None, the function name will be used 
		
		`l_output_names` : list[str] (default = None)
			if None, [`name`] will be used
		"""
		
		# --- dealing with default params ---
		if func is None : func = lambda x:x
		if l_output_name is None : l_output_name = [name]
		if name is None : name = func.__name__
		if func_kwargs is None : func_kwargs = {}

		# --- check common errors ---
		if not self.allow_overwrite :
			for output_names in l_output_name:
				if output_names in self.l_step_name: raise ValueError(f"Step output_name already used ({output_names})")
				if output_names.lower() == 'input' : raise ValueError(f"The step name 'input' is reserved for the input (got {output_names})")
		for input_name in l_input_name :
			if input_name not in self.data : raise ValueError(f"Input not found in dict \n(when looking for the input '{input_name}' for step '{name}')")

		# --- creating the step ---
		step = C_step(	func=func,
						name=name,
						l_input_name=l_input_name,
						l_output_name=l_output_name,
						func_kwargs=func_kwargs)
		
		# --- adding the step ---
		self.l_steps.append(step)
		for name in step.l_output_name : self.data[name]=None

	def execute(self, data=None, dict_data:dict=None):
		"""
		Execute the pipeline on the given data.

		Args:
		----
		`data`: Any
		`dict_data`: dict

		Returns:
		--------
		`output_data`: Any
			Defined by the return value of the last added step
		"""
		if type(data) is dict : dict_data = data

		if dict_data is None : self.data["input"] = data

		else : 
			for data_name, data in dict_data.items():
				if data_name not in self.l_input_names : raise ValueError(f"data_name not in l_input_names (got {data_name})")
				self.data[data_name] = data


		for step in self.l_steps:
			l_arg = [self.data[arg] for arg in step.l_input_name]
			
			result = step.run(l_arg)

			match len(step.l_output_name) :
				case 1 :
					self.data[step.l_output_name[0]] = result
				case N if N>1 : 
					for i,name in enumerate(step.l_output_name) :
						if result is not None : # to avoid problems when raising errors during steps ?
							self.data[name] = result[i]
				case _ :
					raise ValueError(f"Incorect l_output_name (got {step.l_output_name})")
				
		return list(self.data.values())[-1]

	def get_data(self, name):
		"""
		Retrieve the data from a specific step in the pipeline.

		Args
		----
		`name` : str

		Returns
		-------
		`data` : 
			datas associated to `name`
		"""
		return self.data[name]

	@property
	def l_step_name(self):
		return [step.name for step in self.l_steps]
			
	def print_l_step_name(self):
		print("List of all steps names")
		print("-----------------------")
		for name in self.l_step_name :
			print(name)

	def print_l_step_name_with_kwargs(self):
		"""
		same as print_l_step_name, but also print kwargs used by the diffrents functions
		"""
		print("List of all steps names")
		print("  (with func_kwargs)")
		print("-----------------------")
		for step in self.l_steps :
			if len(step.func_kwargs) != 0 :
				func_kwargs_str = "("
				for arg_name,arg in step.func_kwargs.items() :
					func_kwargs_str += f"{arg_name}={arg} , "
				
				func_kwargs_str = func_kwargs_str[:-3] # removing the last " , "
				func_kwargs_str += ")"
			else : func_kwargs_str = ""
			print(f"{step.name}{func_kwargs_str}")

	@property
	def l_data_name(self):
		return list(self.data.keys())
			
	def give_timings(self):
		"""
		Returns
		-------
		- `timings_df` : DataFrame
			- Contain timings information stored in diffrents steps
		"""
		l_steps = self.l_steps
		l_steps_index = []
		l_steps_names = []
		l_steps_times = []
		l_steps_times_per_run = []

		tot_time = sum(step.process_time for step in l_steps)

		for i,step in enumerate(l_steps) :
			l_steps_index.append(i)
			l_steps_names.append(step.name)
			l_steps_times.append(step.process_time)
			l_steps_times_per_run.append(step.process_time / step.nb_run)
		l_steps_times = np.array(l_steps_times)
		l_steps_times_r = 100 * l_steps_times / tot_time

		data = {'Names': l_steps_names, 'Times': l_steps_times, "Time per run" : l_steps_times_per_run, 'Times (%)': l_steps_times_r}
		timings_df = pd.DataFrame(data, index=l_steps_index)
		timings_df.index.name = "step_index"
		return timings_df


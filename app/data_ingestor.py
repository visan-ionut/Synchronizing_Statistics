"""
This module defines the DataIngestor class which is responsible for loading
and processing a dataset from a CSV file. It provides functionalities to
calculate mean statistics for health-related questions across different states.
"""
import pandas as pd
from .logger_config import logger_setup

# Initialize the logger
logger = logger_setup()

class DataIngestor:
    """
    A class to ingest data from a CSV file and perform various analyses on
    health-related data. It supports operations to calculate mean values for
    specific questions across states, categorize states based on their
    statistical outcomes, and more.
    """
    def __init__(self, csv_path: str):
        """
        Constructor for the DataIngestor class.
        Loads the dataset from the specified CSV file and identifies questions for analysis.

        Parameters:
        - csv_path (str): The file path to the CSV data source.
        """
        # Read the dataset from the CSV file into a pandas DataFrame.
        self.data = pd.read_csv(csv_path)

        # Define lists of questions where a lower or higher percentage indicates a 'better' outcome.
        # These categorizations are based on the context and nature of each question.
        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            """Percent of adults who achieve at least 150 minutes a week of moderate-intensity
            aerobicphysical activity or 75 minutes a week of vigorous-intensity aerobic activity
            (or an equivalent combination)""",
            """Percent of adults who achieve at least 150 minutes a week of moderate-intensity
            aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical
            activity and engage in muscle-strengthening activities on 2 or more days a week""",
            """Percent of adults who achieve at least 300 minutes a week of moderate-intensity
            aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity
            (or an equivalent combination)""",
            """Percent of adults who engage in muscle-strengthening activities on 2 or more
            days a week""",
        ]

    def calculate_state_means_and_sort(self, question: str):
        """
        Calculate the mean values for a given question across all states and sort the results.

        Parameters:
        - question (str): The health-related question to analyze.

        Returns:
        - A dictionary of state names with their corresponding mean values,
        sorted by the mean values.
        """
        logger.info("Calculating states_means for question: %s", question)

        # Filter the dataset for the specified question.
        filtered_data = self.data[self.data['Question'] == question]
        # Check if any data exists for the question; if not, log a warning and return
        # an empty dictionary.
        if filtered_data.empty:
            logger.warning("No data found for question: %s", question)
            return {}
        # Group the filtered data by state, calculate the mean for each group,
        # then reset index to turn the results back into a DataFrame.
        state_means = filtered_data.groupby('LocationDesc')['Data_Value'].mean().reset_index()
        # Sort the states based on their mean values according to the determined order.
        sorted_states = state_means.sort_values(by='Data_Value', ascending=True)
        # Convert the sorted DataFrame into a dictionary for easier access and manipulation.
        result = {row['LocationDesc']: row['Data_Value'] for index, row in sorted_states.iterrows()}

        logger.info("Calculated states_means and sorted for question: %s", question)
        return result

    def calculate_mean_for_state(self, question: str, state_name: str):
        """
        Calculates the average value for a specific health-related question
        within a particular state.

        Parameters:
        - question (str): The health-related question to analyze.
        - state_name (str): The name of the state for which the average value is calculated.

        Returns:
        - A dictionary with the state name as the key and the calculated average value as the value.
          If no data is found for the specified question and state, returns an empty dictionary.
        """
        logger.info("Calculating state_means for question: %s and state: %s", question, state_name)

        # Filter the dataset for the specified question and state.
        filtered_data = self.data[(self.data['Question'] == question) & (self.data['LocationDesc']
        == state_name)]
        if filtered_data.empty:
            logger.warning("No data found for question: %s", question)
            return {}
        # Calculate the mean value for the filtered data.
        average_value = filtered_data['Data_Value'].mean()

        logger.info("Calculated average value for question: %s and state: %s", question, state_name)

        return {state_name: average_value}

    def calculate_best5_for_question_order(self, question: str):
        """
        Calculate the top 5 states with the best values for a given question.
        Whether 'best' means highest or lowest values depends on the context of the question.
        """
        logger.info("Calculating best5 for question: %s", question)

        # Determine sorting order based on whether high or low values are considered 'better'
        ascending = question in self.questions_best_is_min
        # Reuse the method to calculate and sort state means
        state_means = self.calculate_state_means_and_sort(question)
        # Sort and select top 5 based on the determined order
        sorted_states = dict(sorted(state_means.items(), key=lambda item: item[1],
        reverse=not ascending))
        top5 = dict(list(sorted_states.items())[:5])

        logger.info("Calculated first 5 values for question: %s", question)
        return top5

    def calculate_worst5_for_question_order(self, question: str):
        """
        Calculate the 5 states with the worst values for a given question.
        Whether 'worst' means highest or lowest values depends on the context of the question.
        """
        logger.info("Calculating worst5 for question: %s", question)

        # Determine sorting order based on whether high or low values are considered 'worse'
        ascending = question in self.questions_best_is_min
        # Reuse the method to calculate and sort state means
        state_means = self.calculate_state_means_and_sort(question)
        # Sort and select bottom 5 based on the determined order
        sorted_states = dict(sorted(state_means.items(), key=lambda item: item[1],
        reverse=not ascending))
        last5 = dict(list(sorted_states.items())[-5:])

        logger.info("Calculated last 5 values for question: %s", question)
        return last5

    def calculate_best5_for_question(self, question: str):
        """
        Identifies the 5 states with the 'best' values for a given
        health-related question.
        'Best' is context-dependent: for some questions, lower values are
        better, and for others, higher values are better.

        Parameters:
        - question (str): The health-related question to analyze.

        Returns:
        - A dictionary of the top 5 states with the best values, based on the question's context.
        """
        return self.calculate_best5_for_question_order(question)

    def calculate_worst5_for_question(self, question: str):
        """
        Identifies the 5 states with the 'worst' values for a given health-related question.
        'Worst' is context-dependent: for some questions, higher values are worse, and for
        others, lower values are worse.

        Parameters:
        - question (str): The health-related question to analyze.

        Returns:
        - A dictionary of the bottom 5 states with the worst values, based on the question's
        context.
        """
        return self.calculate_worst5_for_question_order(question)

    def calculate_global_mean_for_question(self, question: str):
        """
        Calculates the global mean value for a specific health-related question across all states.

        Parameters:
        - question (str): The health-related question to analyze.

        Returns:
        - A dictionary with a key 'global_mean' and the calculated global mean value as its value.
          If no data is found for the specified question, returns an empty dictionary.
        """
        logger.info("Calculating global_mean for question: %s", question)

        # Filter the dataset for the specified question.
        filtered_data = self.data[self.data['Question'] == question]
        if filtered_data.empty:
            logger.warning("No data found for question: %s", question)
            return {}
        # Calculate the global mean value for the filtered data.
        global_mean = filtered_data['Data_Value'].mean()

        logger.info("Calculated global_mean for question: %s", question)
        return {"global_mean": global_mean}

    def calculate_diff_from_mean(self, question: str):
        """
        Calculates the difference of each state's mean value from the global mean for a specific
        health-related question.

        Parameters:
        - question (str): The health-related question for which the differences are calculated.

        Returns:
        - A dictionary where each key is a state and each value is the state's difference from
        the global mean.
          Positive values indicate a state's mean is above the global mean, and negative values
        indicate below.
        """
        logger.info("Calculating diff_from_mean for question: %s", question)

        # Calculate the global mean for the question.
        global_mean = self.calculate_global_mean_for_question(question)["global_mean"]
        # Calculate the mean for each state and compute the difference from the global mean.
        state_means = self.calculate_state_means_and_sort(question)
        diff_from_global = {state: global_mean - mean for state, mean in state_means.items()}

        logger.info("Calculated difference from global mean for question: %s", question)
        return diff_from_global

    def calculate_state_diff_from_global_mean(self, question: str, state_name: str):
        """
        Calculates a specific state's difference from the global mean value for a given
        health-related question.

        Parameters:
        - question (str): The health-related question to analyze.
        - state_name (str): The state for which the difference is calculated.

        Returns:
        - A dictionary with the state as the key and its difference from the global mean as
        the value.
        """
        logger.info("""Calculating state_diff_from_mean for question: %s and state: %s""", question,
        state_name)

        # Calculate the global mean and the state's mean value.
        global_mean = self.calculate_global_mean_for_question(question)["global_mean"]
        state_mean = self.calculate_mean_for_state(question, state_name)[state_name]
        # Compute the difference.
        diff_from_global = (-1) * (state_mean - global_mean)

        logger.info("""Calculated difference from global mean for question: %s and state:
        %s""", question,
        state_name)
        return {state_name: diff_from_global}

    def calculate_mean_by_category(self, question: str):
        """
        Calculates mean values for a given health-related question, segmented by various
        categories within each state.

        Parameters:
        - question (str): The health-related question to analyze.

        Returns:
        - A dictionary where each key represents a combination of state and category, and
        each value is the mean value for that category.
        """
        logger.info("Calculating mean_by_category for question: %s", question)

        # Filter the dataset for the specific question and group by state and category to
        # calculate mean values.
        filtered_data = self.data[self.data['Question'] == question]
        if filtered_data.empty:
            logger.warning("No data found for question: %s", question)
            return {}

        # Group data by state and category, then calculate mean
        grouped_data = filtered_data.groupby(['LocationDesc', 'StratificationCategory1',
        'Stratification1'])['Data_Value'].mean().reset_index()

        # Construct the result dictionary.
        result = {}
        for _, row in grouped_data.iterrows():
            state = row['LocationDesc']
            category = row['StratificationCategory1']
            segment = row['Stratification1']
            mean_value = row['Data_Value']
            key = f"('{state}', '{category}', '{segment}')"
            result[key] = mean_value

        logger.info("""Calculated mean for each segment from each category for question:
        %s""", question)
        return result

    def calculate_state_mean_by_category(self, question: str, state_name: str):
        """
        Calculates mean values for a specific state and question, segmented by various categories.

        Parameters:
        - question (str): The health-related question to analyze.
        - state_name (str): The state for which the mean values are calculated.

        Returns:
        - A dictionary where the key is the state name and the value is another dictionary.
          The nested dictionary's keys are categories and its values are the mean values for those
        categories.
        """
        logger.info("""Calculating state_mean_by_category for question: %s and state:
        %s""", question, state_name)

        # Filter the dataset for the specific question and state, and group by category to calculate
        # mean values.
        filtered_data = self.data[(self.data['Question'] == question) & (self.data['LocationDesc']
        == state_name)]
        if filtered_data.empty:
            logger.warning("No data found for question: %s", question)
            return {}
        # Group data by category, then calculate mean
        grouped_data = filtered_data.groupby(['StratificationCategory1',
        'Stratification1'])['Data_Value'].mean().reset_index()

        # Construct the nested result dictionary.
        result = {state_name: {}}
        for _, row in grouped_data.iterrows():
            category = row['StratificationCategory1']
            segment = row['Stratification1']
            mean_value = row['Data_Value']
            key = (category, segment)
            result[state_name][str(key)] = mean_value

        logger.info("""Calculated mean for each segment from each category for question:
        %s and state: %s""", question, state_name)
        return result

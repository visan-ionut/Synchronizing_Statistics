"""
Unit tests for the DataIngestor class within the app's data_ingestor module.
    
This suite verifies the correct functionality of methods responsible for ingesting,
processing, and analyzing nutritional, activity, and obesity data from a given CSV file.
CSV file is a modified (smaller) version from the original set of data.
"""
import unittest
import os
from app.data_ingestor import DataIngestor

class TestWebserver(unittest.TestCase):
    """
    Test suite for validating the functionality of the DataIngestor class.
    """
    @classmethod
    def setUpClass(cls):
        """
        Sets up the environment before running the tests.
        Here, it initializes the DataIngestor with a particular test CSV file.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(current_dir, "nutrition_activity_obesity_usa_reduced.csv")
        cls.ingestor = DataIngestor(csv_path)

        # Each Test follows the same structure:
        # for a certain question (1 or 2) and certain state_name (1 or 2)
        # calculates the result based on functions from data_ingestor and particular csv file
        # compare with expected which contains the values for each test, calculated manually
        # return the differences between result and expected
        # For more informations about each function, please check data_ingestor

    def test_calculate_state_means_and_sort_1(self):
        """
        Test1 to ensure that the state means are calculated and sorted correctly for
         a specific question.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        expected = {'Arizona': 19.233333333333334, 'Rhode Island': 24.1,
        'North Carolina': 24.1, 'New York': 24.2, 'Utah': 24.9, 'Montana': 26.2,
        'New Mexico': 27.7, 'Ohio': 29.4, 'New Hampshire': 29.5, 'North Dakota': 31.0,
        'Missouri': 31.0, 'Michigan': 32.4, 'Mississippi': 32.5, 'Pennsylvania': 32.7,
        'Indiana': 33.1, 'Georgia': 35.0, 'Nebraska': 36.375, 'California': 36.7,
        'Alabama': 36.94, 'Wyoming': 39.15, 'Tennessee': 40.76666666666667,
        'Arkansas': 42.4, 'Iowa': 44.4}
        result = self.ingestor.calculate_state_means_and_sort(question)
        self.assertEqual(result, expected)

    def test_calculate_state_means_and_sort_2(self):
        """
        Test2 to ensure that the state means are calculated and sorted correctly for
        a specific question.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        expected = {'Rhode Island': 18.599999999999998, 'Kentucky': 18.6,
        'West Virginia': 19.7, 'Pennsylvania': 20.1, 'Tennessee': 21.96666666666667,
        'Missouri': 23.5, 'Louisiana': 23.9, 'North Dakota': 24.7, 'Indiana': 25.8,
        'Alaska': 26.6, 'South Dakota': 27.733333333333334, 'Ohio': 29.150000000000002,
        'Connecticut': 29.3, 'Oregon': 29.525, 'Michigan': 30.85, 'Massachusetts': 31.4,
        'Guam': 34.7, 'Utah': 35.296, 'New Hampshire': 35.3, 'Wyoming': 37.2,
        'Vermont': 37.9, 'National': 38.7, 'Texas': 39.9,'Washington': 40.3,
        'New Mexico': 41.4}
        result = self.ingestor.calculate_state_means_and_sort(question)
        self.assertEqual(result, expected)

    def test_calculate_mean_for_state_1(self):
        """
        Test3 to ensure that the mean for a given state and question are calculated correctly.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        state_name = "Alabama"
        expected = {'Alabama': 36.94}
        result = self.ingestor.calculate_mean_for_state(question, state_name)
        self.assertEqual(result, expected)

    def test_calculate_mean_for_state_2(self):
        """
        Test4 to ensure that the mean for a given state and question are calculated correctly.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        state_name = "Utah"
        expected = {'Utah': 35.296}
        result = self.ingestor.calculate_mean_for_state(question, state_name)
        self.assertEqual(result, expected)

    def test_calculate_best5_for_question_1(self):
        """
        Test5 to ensure that the first 5 values for a given question are calculated correctly.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        expected = {'Arizona': 19.233333333333334, 'Rhode Island': 24.1, 'North Carolina': 24.1,
        'New York': 24.2, 'Utah': 24.9}
        result = self.ingestor.calculate_best5_for_question(question)
        self.assertEqual(result, expected)

    def test_calculate_best5_for_question_2(self):
        """
        Test6 to ensure that the first 5 values for a given question are calculated correctly.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        expected = {'New Mexico': 41.4, 'Washington': 40.3, 'Texas': 39.9, 'National': 38.7,
        'Vermont': 37.9}
        result = self.ingestor.calculate_best5_for_question(question)
        self.assertEqual(result, expected)

    def test_calculate_worst5_for_question_1(self):
        """
        Test7 to ensure that the last 5 values for a given question are calculated correctly.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        expected = {'Alabama': 36.94, 'Wyoming': 39.15, 'Tennessee': 40.76666666666667,
        'Arkansas': 42.4, 'Iowa': 44.4}
        result = self.ingestor.calculate_worst5_for_question(question)
        self.assertEqual(result, expected)

    def test_calculate_worst5_for_questionn_2(self):
        """
        Test8 to ensure that the last 5 values for a given question are calculated correctly.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        expected = {'Tennessee': 21.96666666666667, 'Pennsylvania': 20.1,
        'West Virginia': 19.7, 'Kentucky': 18.6, 'Rhode Island': 18.599999999999998}
        result = self.ingestor.calculate_worst5_for_question(question)
        self.assertEqual(result, expected)

    def test_calculate_global_mean_for_question_1(self):
        """
        Test9 to ensure that the global mean for a given question is calculated correctly.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        expected = {'global_mean': 34.62153846153847}
        result = self.ingestor.calculate_global_mean_for_question(question)
        self.assertEqual(result, expected)

    def test_calculate_global_mean_for_question_2(self):
        """
        Test10 to ensure that the global mean for a given question is calculated correctly.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        expected = {'global_mean': 31.130769230769232}
        result = self.ingestor.calculate_global_mean_for_question(question)
        self.assertEqual(result, expected)

    def test_calculate_calculate_diff_from_mean_1(self):
        """
        Test11 to ensure that the difference from global_mean to all state_mean for a
        given question is calculated correctly.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        expected = {'Arizona': 15.388205128205136, 'Rhode Island': 10.52153846153847,
        'North Carolina': 10.52153846153847, 'New York': 10.421538461538471,
        'Utah': 9.721538461538472, 'Montana': 8.421538461538471, 'New Mexico': 6.921538461538471,
        'Ohio': 5.221538461538472, 'New Hampshire': 5.121538461538471,
        'North Dakota': 3.6215384615384707, 'Missouri': 3.6215384615384707,
        'Michigan': 2.221538461538472, 'Mississippi': 2.1215384615384707,
        'Pennsylvania': 1.9215384615384679, 'Indiana': 1.5215384615384693,
        'Georgia': -0.3784615384615293, 'Nebraska': -1.7534615384615293,
        'California': -2.078461538461532, 'Alabama': -2.318461538461527,
        'Wyoming': -4.528461538461528, 'Tennessee': -6.145128205128202,
        'Arkansas': -7.778461538461528, 'Iowa': -9.778461538461528}
        result = self.ingestor.calculate_diff_from_mean(question)
        self.assertEqual(result, expected)

    def test_calculate_calculate_diff_from_mean_2(self):
        """
        Test12 to ensure that the difference from global_mean to all state_mean for a
        given question is calculated correctly.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        expected = {'Rhode Island': 12.530769230769234, 'Kentucky': 12.53076923076923,
        'West Virginia': 11.430769230769233, 'Pennsylvania': 11.03076923076923,
        'Tennessee': 9.164102564102564, 'Missouri': 7.630769230769232,
        'Louisiana': 7.2307692307692335, 'North Dakota': 6.430769230769233,
        'Indiana': 5.330769230769231, 'Alaska': 4.530769230769231,
        'South Dakota': 3.397435897435898, 'Ohio': 1.98076923076923,
        'Connecticut': 1.8307692307692314, 'Oregon': 1.6057692307692335,
        'Michigan': 0.28076923076923066, 'Massachusetts': -0.2692307692307665,
        'Guam': -3.5692307692307708, 'Utah': -4.165230769230767,
        'New Hampshire': -4.169230769230765, 'Wyoming': -6.069230769230771,
        'Vermont': -6.7692307692307665, 'National': -7.569230769230771,
        'Texas': -8.769230769230766, 'Washington': -9.169230769230765,
        'New Mexico': -10.269230769230766}
        result = self.ingestor.calculate_diff_from_mean(question)
        self.assertEqual(result, expected)

    def test_calculate_state_diff_from_global_mean_1(self):
        """
        Test13 to ensure that the difference from global_mean to a certain state_name
        and a given question is calculated correctly.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        state_name = "Alabama"
        expected = {'Alabama': -2.318461538461527}
        result = self.ingestor.calculate_state_diff_from_global_mean(question, state_name)
        self.assertEqual(result, expected)

    def test_calculate_state_diff_from_global_mean_2(self):
        """
        Test14 to ensure that the difference from global_mean to a certain state_name
        and a given question is calculated correctly.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        state_name = "Utah"
        expected = {'Utah': -4.165230769230767}
        result = self.ingestor.calculate_state_diff_from_global_mean(question, state_name)
        self.assertEqual(result, expected)

    def test_calculate_mean_by_category_1(self):
        """
        Test15 to ensure that the mean values for a given health-related question,
        segmented by various categories within each state is calculated correctly.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        expected = {"('Alabama', 'Age (years)', '25 - 34')": 31.3,
        "('Alabama', 'Age (years)', '35 - 44')": 43.2,
        "('Alabama', 'Age (years)', '45 - 54')": 43.400000000000006,
        "('Alabama', 'Education', 'High school graduate')": 34.7,
        "('Alabama', 'Education', 'Less than high school')": 40.5,
        "('Alabama', 'Income', '$15,000 - $24,999')": 40.8,
        "('Alabama', 'Income', '$35,000 - $49,999')": 38.3,
        "('Alabama', 'Income', '$50,000 - $74,999')": 35.599999999999994,
        "('Alabama', 'Income', '$75,000 or greater')": 32.0,
        "('Alabama', 'Race/Ethnicity', 'American Indian/Alaska Native')": 46.2,
        "('Alabama', 'Race/Ethnicity', 'Asian')": 31.0,
        "('Alabama', 'Race/Ethnicity', 'Hispanic')": 30.7,
        "('Alabama', 'Race/Ethnicity', 'Non-Hispanic Black')": 43.166666666666664,
        "('Alabama', 'Race/Ethnicity', 'Non-Hispanic White')": 30.1,
        "('Alabama', 'Race/Ethnicity', 'Other')": 31.0,
        "('Arizona', 'Race/Ethnicity', 'Asian')": 14.6,
        "('Arizona', 'Race/Ethnicity', 'Non-Hispanic White')": 28.5,
        "('Arkansas', 'Education', 'College graduate')": 27.2,
        "('Arkansas', 'Race/Ethnicity', 'Non-Hispanic Black')": 50.0,
        "('California', 'Education', 'Less than high school')": 36.7,
        "('Georgia', 'Education', 'High school graduate')": 35.0,
        "('Indiana', 'Age (years)', '65 or older')": 33.1,
        "('Iowa', 'Income', '$50,000 - $74,999')": 41.0,
        "('Iowa', 'Income', 'Less than $15,000')": 46.1,
        "('Michigan', 'Education', 'Some college or technical school')": 32.4,
        "('Mississippi', 'Income', '$50,000 - $74,999')": 33.9,
        "('Mississippi', 'Race/Ethnicity', 'Non-Hispanic White')": 31.1,
        "('Missouri', 'Race/Ethnicity', 'Other')": 31.0,
        "('Montana', 'Education', 'Less than high school')": 26.2,
        "('Nebraska', 'Education', 'High school graduate')": 35.1,
        "('Nebraska', 'Income', 'Less than $15,000')": 44.3,
        "('Nebraska', 'Race/Ethnicity', 'Hawaiian/Pacific Islander')": 31.0,
        "('New Hampshire', 'Income', '$25,000 - $34,999')": 29.5,
        "('New Mexico', 'Income', '$25,000 - $34,999')": 27.7,
        "('New York', 'Race/Ethnicity', 'American Indian/Alaska Native')": 24.2,
        "('North Carolina', 'Education', 'College graduate')": 24.1,
        "('North Dakota', 'Race/Ethnicity', 'Other')": 31.0,
        "('Ohio', 'Income', '$75,000 or greater')": 29.4,
        "('Pennsylvania', 'Gender', 'Male')": 32.7,
        "('Rhode Island', 'Gender', 'Female')": 24.1,
        "('Tennessee', 'Income', 'Less than $15,000')": 44.1,
        "('Tennessee', 'Race/Ethnicity', 'Hispanic')": 39.1,
        "('Utah', 'Gender', 'Male')": 24.9,
        "('Wyoming', 'Income', '$15,000 - $24,999')": 33.9,
        "('Wyoming', 'Race/Ethnicity', 'American Indian/Alaska Native')": 44.4}
        result = self.ingestor.calculate_mean_by_category(question)
        self.assertEqual(result, expected)

    def test_calculate_mean_by_category_2(self):
        """
        Test16 to ensure that the mean values for a given health-related question,
        segmented by various categories within each state is calculated correctly.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        expected = {"('Alaska', 'Age (years)', '55 - 64')": 26.6,
        "('Connecticut', 'Income', '$25,000 - $34,999')": 29.3,
        "('Guam', 'Race/Ethnicity', 'Hispanic')": 34.7,
        "('Indiana', 'Age (years)', '45 - 54')": 25.8,
        "('Kentucky', 'Age (years)', '65 or older')": 18.6,
        "('Louisiana', 'Education', 'Less than high school')": 23.9,
        "('Massachusetts', 'Age (years)', '35 - 44')": 31.4,
        "('Michigan', 'Gender', 'Male')": 33.1,
        "('Michigan', 'Income', '$50,000 - $74,999')": 28.6,
        "('Missouri', 'Gender', 'Female')": 23.5,
        "('National', 'Age (years)', '25 - 34')": 38.7,
        "('New Hampshire', 'Gender', 'Female')": 35.3,
        "('New Mexico', 'Race/Ethnicity', 'American Indian/Alaska Native')": 41.4,
        "('North Dakota', 'Gender', 'Female')": 24.7,
        "('Ohio', 'Race/Ethnicity', 'Hispanic')": 23.6,
        "('Ohio', 'Total', 'Total')": 34.7,
        "('Oregon', 'Age (years)', '55 - 64')": 25.4,
        "('Oregon', 'Race/Ethnicity', 'Hawaiian/Pacific Islander')": 30.9,
        "('Oregon', 'Race/Ethnicity', 'Other')": 30.9,
        "('Pennsylvania', 'Age (years)', '65 or older')": 20.1,
        "('Rhode Island', 'Income', 'Less than $15,000')": 18.599999999999998,
        "('South Dakota', 'Education', 'Some college or technical school')": 31.6,
        "('South Dakota', 'Income', '$35,000 - $49,999')": 25.8,
        "('Tennessee', 'Age (years)', '65 or older')": 17.5,
        "('Tennessee', 'Race/Ethnicity', 'American Indian/Alaska Native')": 30.9,
        "('Texas', 'Income', '$35,000 - $49,999')": 39.9,
        "('Utah', 'Age (years)', '45 - 54')": 29.4,
        "('Utah', 'Education', 'College graduate')": 36.1,
        "('Utah', 'Education', 'High school graduate')": 29.7,
        "('Utah', 'Gender', 'Female')": 30.5,
        "('Utah', 'Gender', 'Male')": 38.2,
        "('Utah', 'Income', '$15,000 - $24,999')": 26.8,
        "('Utah', 'Income', '$25,000 - $34,999')": 28.2,
        "('Utah', 'Income', '$50,000 - $74,999')": 35.3,
        "('Utah', 'Income', '$75,000 or greater')": 39.2,
        "('Utah', 'Income', 'Data not reported')": 30.3,
        "('Utah', 'Income', 'Less than $15,000')": 27.8,
        "('Utah', 'Race/Ethnicity', '2 or more races')": 54.7,
        "('Utah', 'Race/Ethnicity', 'American Indian/Alaska Native')": 43.2,
        "('Utah', 'Race/Ethnicity', 'Asian')": 45.6,
        "('Utah', 'Race/Ethnicity', 'Hawaiian/Pacific Islander')": 30.9,
        "('Utah', 'Race/Ethnicity', 'Hispanic')": 26.599999999999998,
        "('Utah', 'Race/Ethnicity', 'Non-Hispanic Black')": 42.7,
        "('Utah', 'Race/Ethnicity', 'Non-Hispanic White')": 38.1,
        "('Vermont', 'Education', 'Less than high school')": 37.9,
        "('Washington', 'Income', '$75,000 or greater')": 40.3,
        "('West Virginia', 'Education', 'High school graduate')": 19.7,
        "('Wyoming', 'Age (years)', '25 - 34')": 37.2}
        result = self.ingestor.calculate_mean_by_category(question)
        self.assertEqual(result, expected)

    def test_calculate_state_mean_by_category_1(self):
        """
        Test17 to ensure that the mean values for a specific state and question,
        segmented by various categories is calculated correctly.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        state_name = "Alabama"
        expected = {'Alabama': {"('Age (years)', '25 - 34')": 31.3,
        "('Age (years)', '35 - 44')": 43.2,
        "('Age (years)', '45 - 54')": 43.400000000000006,
        "('Education', 'High school graduate')": 34.7,
        "('Education', 'Less than high school')": 40.5,
        "('Income', '$15,000 - $24,999')": 40.8,
        "('Income', '$35,000 - $49,999')": 38.3,
        "('Income', '$50,000 - $74,999')": 35.599999999999994,
        "('Income', '$75,000 or greater')": 32.0,
        "('Race/Ethnicity', 'American Indian/Alaska Native')": 46.2,
        "('Race/Ethnicity', 'Asian')": 31.0,
        "('Race/Ethnicity', 'Hispanic')": 30.7,
        "('Race/Ethnicity', 'Non-Hispanic Black')": 43.166666666666664,
        "('Race/Ethnicity', 'Non-Hispanic White')": 30.1,
        "('Race/Ethnicity', 'Other')": 31.0}}
        result = self.ingestor.calculate_state_mean_by_category(question, state_name)
        self.assertEqual(result, expected)

    def test_calculate_state_mean_by_category_2(self):
        """
        Test18 to ensure that the mean values for a specific state and question,
        segmented by various categories is calculated correctly.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on "\
        "2 or more days a week"
        state_name = "Utah"
        expected = {'Utah': {"('Age (years)', '45 - 54')": 29.4,
        "('Education', 'College graduate')": 36.1,
        "('Education', 'High school graduate')": 29.7,
        "('Gender', 'Female')": 30.5, "('Gender', 'Male')": 38.2,
        "('Income', '$15,000 - $24,999')": 26.8,
        "('Income', '$25,000 - $34,999')": 28.2,
        "('Income', '$50,000 - $74,999')": 35.3,
        "('Income', '$75,000 or greater')": 39.2,
        "('Income', 'Data not reported')": 30.3,
        "('Income', 'Less than $15,000')": 27.8,
        "('Race/Ethnicity', '2 or more races')": 54.7,
        "('Race/Ethnicity', 'American Indian/Alaska Native')": 43.2,
        "('Race/Ethnicity', 'Asian')": 45.6,
        "('Race/Ethnicity', 'Hawaiian/Pacific Islander')": 30.9,
        "('Race/Ethnicity', 'Hispanic')": 26.599999999999998,
        "('Race/Ethnicity', 'Non-Hispanic Black')": 42.7,
        "('Race/Ethnicity', 'Non-Hispanic White')": 38.1}}
        result = self.ingestor.calculate_state_mean_by_category(question, state_name)
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()

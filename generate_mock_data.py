import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_mock_data():
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Generate data for the last 3 years
    years = [2022, 2023, 2024]
    programs = ['MGB', 'GMBA']
    campuses = ['SG', 'DXB', 'MUM']
    cohorts = ['Jan-22', 'May-22', 'Sep-22', 'Jan-23', 'May-23', 'Sep-23', 'Jan-24', 'May-24', 'Sep-24']
    
    # 1. AI Tutor Usage Summary Template
    ai_tutor_data = []
    for year in years:
        for program in programs:
            for campus in campuses:
                for cohort in cohorts:
                    if int(cohort.split('-')[1]) == year % 100:
                        for unit in range(1, 6):  # 5 units per cohort
                            ai_tutor_data.append({
                                'Campus': campus,
                                'Course_Name': program,
                                'Cohort': cohort,
                                'Unit_Name': f'Unit {unit}',
                                'Faculty': f'Prof_{random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"])}',
                                'Email_ID': f'prof_{random.choice(["smith", "johnson", "williams", "brown", "jones"])}@spjain.edu',
                                'Unit_Commencement_date': f'{random.randint(1, 28)}-{random.choice(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}-{year}',
                                'No_of_Session_IDs_created': random.randint(10, 25),
                                'Total_Students_Participated': random.randint(80, 120),
                                'Batch_size': random.randint(100, 150),
                                'Student_adoption_rate': random.uniform(70, 95),
                                'End_Date': f'{random.randint(1, 28)}-{random.choice(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}-{year}',
                                'No_of_students_who_filled_form': random.randint(60, 100),
                                'Size_of_batch_when_feedback_taken': random.randint(80, 120),
                                'Faculty_Avg_Score': random.uniform(3.5, 4.8),
                                'AI_Tutor_quality_score': random.uniform(3.8, 4.9),
                                'AI_Tutor_impact_score': random.uniform(3.7, 4.8),
                                'Avg_Rating_for_AI_Tutor_Tool': random.uniform(3.5, 4.9),
                                'Implemented_AI_Tutor': random.choice(['Y', 'N']),
                                'Features_found_useful': random.choice(['Document Creator', 'Quiz Generator', 'Study Planner', 'All Features']),
                                'Used_Document_Creator': random.choice(['Y', 'N']),
                                'Quizzes_conducted': random.randint(0, 8),
                                'Quizzes_used_for_grading': random.randint(0, 5),
                                'Quiz_outcome': random.choice(['Excellent', 'Good', 'Satisfactory', 'Needs Improvement']),
                                'Faculty_Feedback': random.choice(['Very Positive', 'Positive', 'Neutral', 'Some Concerns'])
                            })
    
    # 2. AI Mentor Feedback Template
    ai_mentor_data = []
    for year in years:
        for program in programs:
            for cohort in cohorts:
                if int(cohort.split('-')[1]) == year % 100:
                    for _ in range(random.randint(3, 8)):  # 3-8 academic managers per cohort
                        ai_mentor_data.append({
                            'Academic_Manager_Name': f'AM_{random.choice(["Kumar", "Singh", "Patel", "Sharma", "Verma"])}',
                            'Program': program,
                            'Cohort': cohort,
                            'Term': random.choice(['Term 1', 'Term 2', 'Term 3', 'Term 4']),
                            'Q1_Improvement_observed': random.choice(['Strongly Agree', 'Agree', 'Neutral', 'Disagree']),
                            'Q2_Students_motivated': random.choice(['Strongly Agree', 'Agree', 'Neutral', 'Disagree']),
                            'Q3_Effectiveness': random.choice(['Very Effective', 'Effective', 'Somewhat Effective', 'Not Effective']),
                            'Q4_Key_observations': random.choice([
                                'Students showed significant improvement in project quality',
                                'AI Mentor helped students stay on track',
                                'Some students needed additional guidance',
                                'AI Mentor reduced faculty workload effectively'
                            ])
                        })
    
    # 3. AI Initiatives Impact
    ai_impact_data = []
    for year in years:
        for program in programs:
            for cohort in cohorts:
                if int(cohort.split('-')[1]) == year % 100:
                    for student_id in range(1, random.randint(80, 120)):
                        ai_impact_data.append({
                            'Student_Name': f'Student_{student_id}_{cohort}',
                            'Student_mail_id': f'student{student_id}@{cohort.lower()}.spjain.edu',
                            'Program': program,
                            'Cohort': cohort,
                            'Term': random.choice(['Term 1', 'Term 2', 'Term 3', 'Term 4']),
                            'Placed_Not_Placed': random.choice(['Placed', 'Not Placed']),
                            'CGPA': round(random.uniform(2.5, 4.0), 2),
                            'AI_Tutor_Usage': random.choice(['High', 'Medium', 'Low', 'None']),
                            'AI_Mentor_Usage': random.choice(['High', 'Medium', 'Low', 'None']),
                            'AI_TKT_Exam_Usage': random.choice(['High', 'Medium', 'Low', 'None']),
                            'JPT_Usage': random.choice(['High', 'Medium', 'Low', 'None']),
                            'Yoodli_Usage': random.choice(['High', 'Medium', 'Low', 'None'])
                        })
    
    # 4. JPT Usage Analysis and Placement Tracker
    jpt_data = []
    companies = ['Google', 'Microsoft', 'Amazon', 'Apple', 'Meta', 'Netflix', 'IBM', 'Oracle', 'Salesforce', 'Adobe']
    industries = ['Technology', 'Finance', 'Consulting', 'Healthcare', 'Retail', 'Manufacturing', 'Education']
    company_tiers = ['Tier 1', 'Tier 2', 'Tier 3']
    job_roles = ['Data Analyst', 'Business Analyst', 'Product Manager', 'Consultant', 'Marketing Manager', 'Operations Manager']
    locations = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai', 'Pune', 'Gurgaon']
    
    for year in years:
        for program in programs:
            for cohort in cohorts:
                if int(cohort.split('-')[1]) == year % 100:
                    for company in random.sample(companies, random.randint(5, 10)):
                        vacancies_offered = random.randint(2, 15)
                        students_eligible = random.randint(20, 50)
                        applied = random.randint(10, 30)
                        interviewed = random.randint(5, 20)
                        selected = random.randint(1, min(vacancies_offered, 8))
                        
                        jpt_data.append({
                            'Year': year,
                            'Program': program,
                            'Cohort': cohort,
                            'Company': company,
                            'Industry_Sector': random.choice(industries),
                            'Company_Tier': random.choice(company_tiers),
                            'Job_role': random.choice(job_roles),
                            'Location': random.choice(locations),
                            'Students_Eligible': students_eligible,
                            'Applied_Y_N': applied,
                            'Students_Interviewed': interviewed,
                            'Vacancies_Offered': vacancies_offered,
                            'Students_Selected': selected,
                            'Avg_CTC': random.randint(800000, 2500000),
                            'Highest_CTC': random.randint(1200000, 3500000)
                        })
    
    # 5. Unit-wise Performance Analysis
    unit_performance_data = []
    for year in years:
        for program in programs:
            for cohort in cohorts:
                if int(cohort.split('-')[1]) == year % 100:
                    for unit in range(1, 6):
                        cp = random.uniform(15, 20)
                        ia = random.uniform(15, 20)
                        ga = random.uniform(15, 20)
                        te = random.uniform(30, 40)
                        total = cp + ia + ga + te
                        
                        unit_performance_data.append({
                            'Unit_Name': f'Unit {unit}',
                            'CP': round(cp, 2),
                            'IA': round(ia, 2),
                            'GA': round(ga, 2),
                            'TE': round(te, 2),
                            'Total_score': round(total, 2),
                            'Year': year,
                            'Program': program,
                            'Cohort': cohort
                        })
    
    # Convert to DataFrames
    ai_tutor_df = pd.DataFrame(ai_tutor_data)
    ai_mentor_df = pd.DataFrame(ai_mentor_data)
    ai_impact_df = pd.DataFrame(ai_impact_data)
    jpt_df = pd.DataFrame(jpt_data)
    unit_performance_df = pd.DataFrame(unit_performance_data)
    
    # Save to CSV files
    ai_tutor_df.to_csv('ai_tutor_mock_data.csv', index=False)
    ai_mentor_df.to_csv('ai_mentor_mock_data.csv', index=False)
    ai_impact_df.to_csv('ai_impact_mock_data.csv', index=False)
    jpt_df.to_csv('jpt_mock_data.csv', index=False)
    unit_performance_df.to_csv('unit_performance_mock_data.csv', index=False)
    
    print("Mock data generated successfully!")
    print(f"AI Tutor data: {len(ai_tutor_data)} records")
    print(f"AI Mentor data: {len(ai_mentor_data)} records")
    print(f"AI Impact data: {len(ai_impact_data)} records")
    print(f"JPT data: {len(jpt_data)} records")
    print(f"Unit Performance data: {len(unit_performance_data)} records")
    
    return ai_tutor_df, ai_mentor_df, ai_impact_df, jpt_df, unit_performance_df

if __name__ == "__main__":
    generate_mock_data()

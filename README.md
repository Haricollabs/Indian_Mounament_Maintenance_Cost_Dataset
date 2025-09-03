# Indian_Mounament_Maintenance_Cost_Dataset
Dataset Source:
              The Dataset is obtained from Indian Government's official website for the period of 2018-2023 Financial Years.
              (https://www.data.gov.in/resource/stateut-wise-allocation-expenditure-incurred-conservation-preservation-and-environmental)

              The Dataset contains informartion about Indian Government's statewise allocation/expenditure for Mounament Maintenance each year.

              The Initia Schema before transformations was: State/UT STRING, 2018_19_Allocation Decimal (10,0), 2018_19_Expenditure Decimal(10,0)...so on

Tools Used:
              Databricks, Power BI

Applied Transformations:
                        Convert allocation/expenditure columns to represent value in Lakhs
                        Unpivot Allocation and Expenditure columns to create Type column
                        Split type column into 2 columns - Year and Type (allocation/expenditure)
                        Pivot type column to Allocated amount/Spent Amount
                        
                        
                                              

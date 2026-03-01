def calculate_retirement_data(retirement_tracker_config):
    retirement_year = int(retirement_tracker_config["retirement_year"])
    end_year = int(retirement_tracker_config["end_year"])
    inflation = float(retirement_tracker_config["inflation"]) / 100
    rate_of_interest = float(retirement_tracker_config["rate_of_interest"]) / 100
    tax = float(retirement_tracker_config["tax"]) / 100
    yearly_expenses = float(retirement_tracker_config["yearly_expenses"])
    investment_amount = float(retirement_tracker_config["investment_amount"])
    retirement_data = []

    for year in range(retirement_year, end_year):
        number_of_years_since_retirement = year - retirement_year

        inflation_adjusted_yearly_expenses = (
            yearly_expenses * (1 + inflation) ** number_of_years_since_retirement
        )

        investment_amount_current_year = investment_amount
        investment_future_value = investment_amount_current_year + (
            investment_amount_current_year * rate_of_interest
        )

        income = investment_future_value - investment_amount_current_year
        tax_current_year = income * tax

        investment_amount = (
            investment_future_value
            - tax_current_year
            - inflation_adjusted_yearly_expenses
        )

        retirement_data.append(
            {
                "YEAR": year,
                "INFLATION (%)": round(inflation * 100, 2),
                "RATE OF INTEREST (%)": round(rate_of_interest * 100, 2),
                "TAX (%)": round(tax * 100, 2),
                "BASE YEARLY EXPENSES": round(yearly_expenses, 2),
                "INVESTMENT AMOUNT": round(investment_amount_current_year, 2),
                "INFLATION ADJUSTED YEARLY EXPENSES": round(
                    inflation_adjusted_yearly_expenses, 2
                ),
                "INCOME": round(income, 2),
                "TAX AMOUNT": round(tax_current_year, 2),
                "INVESTMENT AMOUNT FOR NEXT YEAR": round(investment_amount, 2),
            }
        )

    return retirement_data

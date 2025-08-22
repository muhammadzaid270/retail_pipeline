| Column Name         | Data Type | optional | Description |
|---------------------|----------|----------|-------------|
| Date                | datetime | no       | Date when the order was placed |
| Customer ID         | string   | no       | Unique identifier for the customer |
| Customer Name       | string   | no       | Full name of the customer |
| Product ID          | string   | no       | Unique identifier for the product |
| Product Description | string   | no       | Description of the product |
| Quantity            | int      | no       | Number of units ordered (pcs) |
| Price               | float    | no       | Price per unit (USD) |
| Total               | float    | no       | Calculated as Quantity × Price |
| Sales_Rep           | string   | no       | Salesperson handling the order |
| Region              | string   | no       | Region where the sale occurred |
| Payment Method       | string   | yes       | Payment method (cash, card, bank) |
| Commission          | float    | no       | Commission percentage of sale (convert to decimal first) |
| Email               | string   | yes      | Customer's email address |
| Phone               | string   | yes       | Customer's phone number |
| Shipping Address     | string   | yes       | Delivery address for the order |
| Order Priority      | string   | yes       | Priority of the order (High/Low) |
| Tax Amount          | float    | no       | Tax charged |
| Notes          | string    | yes       | Notes |

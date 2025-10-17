"""
Gerador de dados sintéticos de e-commerce
Formato otimizado para PySpark (JSON Lines)
"""

from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import uuid
import os

fake = Faker('pt_BR')
Faker.seed(42)
random.seed(42)

class EcommerceDataGenerator:
    """Gerador de dados sintéticos para e-commerce"""
    
    def __init__(self, num_customers=1000, num_products=500):
        self.num_customers = num_customers
        self.num_products = num_products
        self.customers = []
        self.products = []
        
        print(f"\n{'='*70}")
        print(f"🎲 GERADOR DE DADOS SINTÉTICOS - E-COMMERCE")
        print(f"{'='*70}")
        print(f"📊 Configuração:")
        print(f"   👥 Clientes: {num_customers:,}")
        print(f"   📦 Produtos: {num_products:,}")
        print(f"{'='*70}\n")
        
    def generate_customers(self):
        """Gera dados de clientes"""
        print(f"👥 Gerando {self.num_customers:,} clientes...")
        
        customers = []
        duplicates_count = 0
        
        for i in range(self.num_customers):
            if random.random() < 0.05 and i > 0:
                customer = customers[-1].copy()
                customer['email'] = fake.email()
                duplicates_count += 1
            else:
                birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)
                created_at = fake.date_time_between(start_date='-2y', end_date='now')
                
                customer = {
                    'customer_id': str(uuid.uuid4()),
                    'name': fake.name(),
                    'email': fake.email(),
                    'phone': fake.phone_number(),
                    'cpf': fake.cpf(),
                    'birth_date': birth_date.strftime('%Y-%m-%d'),
                    'age': (datetime.now().date() - birth_date).days // 365,
                    'gender': random.choice(['M', 'F', 'Other']),
                    'address': fake.street_address(),
                    'city': fake.city(),
                    'state': fake.state_abbr(),
                    'zip_code': fake.postcode(),
                    'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': fake.date_time_between(start_date=created_at, end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
                    'is_active': random.choice([True, True, True, False])
                }
            customers.append(customer)
        
        self.customers = customers
        print(f"   ✅ {len(customers):,} clientes gerados")
        print(f"   ⚠️  {duplicates_count} duplicatas intencionais")
        
        return pd.DataFrame(customers)
    
    def generate_products(self):
        """Gera catálogo de produtos"""
        print(f"\n📦 Gerando {self.num_products:,} produtos...")
        
        categories = {
            'Eletrônicos': ['Smartphone', 'Notebook', 'Tablet'],
            'Moda': ['Camiseta', 'Calça', 'Tênis'],
            'Casa': ['Sofá', 'Mesa', 'Cadeira']
        }
        
        price_ranges = {
            'Eletrônicos': (500, 5000),
            'Moda': (50, 500),
            'Casa': (200, 3000)
        }
        
        products = []
        
        for _ in range(self.num_products):
            category = random.choice(list(categories.keys()))
            subcategory = random.choice(categories[category])
            min_price, max_price = price_ranges[category]
            
            price = round(random.uniform(min_price, max_price), 2)
            
            product = {
                'product_id': str(uuid.uuid4()),
                'product_name': f"{subcategory} {fake.company()}",
                'category': category,
                'price': price,
                'stock_quantity': random.randint(0, 500),
                'created_at': fake.date_time_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
            }
            products.append(product)
        
        self.products = products
        print(f"   ✅ {len(products):,} produtos gerados")
        
        return pd.DataFrame(products)
    
    def generate_sales(self, num_orders=5000):
        """Gera histórico de vendas"""
        print(f"\n🛒 Gerando {num_orders:,} pedidos...")
        
        if not self.customers or not self.products:
            raise ValueError("❌ Gere clientes e produtos primeiro!")
        
        sales = []
        
        for _ in range(num_orders):
            customer = random.choice(self.customers)
            product = random.choice(self.products)
            quantity = random.randint(1, 5)
            
            sale = {
                'order_id': str(uuid.uuid4()),
                'customer_id': customer['customer_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'total_amount': round(quantity * product['price'], 2),
                'status': random.choice(['completed', 'pending', 'cancelled']),
                'order_date': fake.date_time_between(start_date='-6m', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
            }
            sales.append(sale)
        
        print(f"   ✅ {len(sales):,} pedidos gerados")
        
        return pd.DataFrame(sales)
    
    def save_to_files(self, output_path="data/raw"):
        """Salva todos os dados gerados"""
        print(f"\n{'='*70}")
        print(f"�� SALVANDO DADOS")
        print(f"{'='*70}\n")
        
        os.makedirs(output_path, exist_ok=True)
        
        df_customers = self.generate_customers()
        df_products = self.generate_products()
        df_sales = self.generate_sales(num_orders=5000)
        
        # Salvar JSON em formato "lines" (uma linha por registro)
        df_customers.to_json(f"{output_path}/customers.json", orient='records', lines=True, date_format='iso')
        df_products.to_csv(f"{output_path}/products.csv", index=False)
        df_sales.to_json(f"{output_path}/sales.json", orient='records', lines=True, date_format='iso')
        
        print(f"\n✅ DADOS SALVOS COM SUCESSO!")
        print(f"   👥 Clientes: {len(df_customers):,}")
        print(f"   📦 Produtos: {len(df_products):,}")
        print(f"   🛒 Vendas: {len(df_sales):,}")
        print(f"{'='*70}\n")
        
        return {'customers': df_customers, 'products': df_products, 'sales': df_sales}

if __name__ == "__main__":
    generator = EcommerceDataGenerator(num_customers=1000, num_products=500)
    datasets = generator.save_to_files()
    print("🎉 CONCLUÍDO!")

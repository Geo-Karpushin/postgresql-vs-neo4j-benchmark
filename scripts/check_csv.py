import pandas as pd
df = pd.read_csv("generated/very-small/friendships.csv")
print(f"Строк: {len(df):,}")
print(f"Первые 100 строк:")
print(df.head())
print(f"\nПроверяем пары:")
# Если пары канонические, то user_id всегда меньше friend_id?
print(f"user_id < friend_id: {(df['user_id'] < df['friend_id']).all()}")
from distributor import Distributor

distributor = Distributor()
distributor.set_analyzer({'id': 'analyzer1', 'weight': 0.5, 'online': True})
distributor.set_analyzer({'id': 'analyzer2', 'weight': 0.3, 'online': True})
distributor.set_analyzer({'id': 'analyzer3', 'weight': 0.2, 'online': True})

# Simulate incoming requests with sequential request IDs
for i in range(100):
    distributor.distribute_message(i)
print(distributor.message_counts)
    
distributor.set_analyzer({'id': 'analyzer1', 'weight': 0.5, 'online': False})

for i in range(100):
    distributor.distribute_message(i)
print(distributor.message_counts)


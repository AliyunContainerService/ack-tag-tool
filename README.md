# ACK Tag Tool

Tag all Alibaba Cloud resources used in specific ACK K8s cluster

# Setup

```
pip3 install -r requirements.txt
```

# Usage

Set environment variables for AK

```
export ACCESS_KEY_ID=xxxxxx
export ACCESS_KEY_SECRET=xxxxxx
```

Check resource tag for specific ACK K8s cluster

```
python3 main.py  --cluster_id=xxxxxx --region=cn-beijing --key=test-key --value=test-value
```

Tag resource for specific ACK K8s cluster

```
python3 main.py  --cluster_id=xxxxxx --region=cn-beijing --key=test-key --value=test-value -s
```
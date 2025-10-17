#!/usr/bin/env python3
"""
Script to add EC2 instance metadata (instance type and pricing) to benchmark result JSON files.

This script:
1. Fetches EC2 instance type from the instance metadata service (IMDS)
2. Fetches current on-demand pricing for the instance type from AWS Pricing API
3. Adds 'ec2_instance_type' and 'usd_per_hour' fields to the JSON result file

Usage:
    python3 add_ec2_metadata.py <result_file.json>
    
Example:
    python3 add_ec2_metadata.py duckdb/results/internal/tpch-sf1000-internal-results.json
"""

import json
import sys
import urllib.request
import urllib.error
from typing import Optional, Dict, Any


def get_ec2_instance_type() -> Optional[str]:
    """
    Fetch EC2 instance type from the instance metadata service (IMDSv2).
    
    Returns:
        Instance type string (e.g., 'r6gd.metal') or None if not running on EC2
    """
    try:
        # IMDSv2 requires a token
        token_url = "http://169.254.169.254/latest/api/token"
        token_request = urllib.request.Request(
            token_url,
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            method="PUT"
        )
        
        with urllib.request.urlopen(token_request, timeout=2) as response:
            token = response.read().decode('utf-8')
        
        # Use token to get instance type
        metadata_url = "http://169.254.169.254/latest/meta-data/instance-type"
        metadata_request = urllib.request.Request(
            metadata_url,
            headers={"X-aws-ec2-metadata-token": token}
        )
        
        with urllib.request.urlopen(metadata_request, timeout=2) as response:
            instance_type = response.read().decode('utf-8').strip()
        
        return instance_type
    
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        print(f"Warning: Could not fetch EC2 instance type from metadata service: {e}")
        print("This is normal if not running on EC2.")
        return None


def get_ec2_region() -> Optional[str]:
    """
    Fetch EC2 region from the instance metadata service (IMDSv2).
    
    Returns:
        Region string (e.g., 'us-east-1') or None if not running on EC2
    """
    try:
        # IMDSv2 requires a token
        token_url = "http://169.254.169.254/latest/api/token"
        token_request = urllib.request.Request(
            token_url,
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            method="PUT"
        )
        
        with urllib.request.urlopen(token_request, timeout=2) as response:
            token = response.read().decode('utf-8')
        
        # Use token to get availability zone
        metadata_url = "http://169.254.169.254/latest/meta-data/placement/availability-zone"
        metadata_request = urllib.request.Request(
            metadata_url,
            headers={"X-aws-ec2-metadata-token": token}
        )
        
        with urllib.request.urlopen(metadata_request, timeout=2) as response:
            az = response.read().decode('utf-8').strip()
        
        # Extract region from AZ (e.g., 'us-east-1a' -> 'us-east-1')
        region = az[:-1]
        return region
    
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        print(f"Warning: Could not fetch EC2 region from metadata service: {e}")
        return None


def get_ec2_pricing(instance_type: str, region: str) -> Optional[float]:
    """
    Fetch current on-demand pricing for an EC2 instance type using AWS Pricing API.
    
    Args:
        instance_type: EC2 instance type (e.g., 'r6gd.metal')
        region: AWS region (e.g., 'us-east-1')
    
    Returns:
        Price per hour in USD, or None if pricing could not be fetched
    """
    try:
        # AWS Pricing API is only available in us-east-1
        # Map region codes to pricing API region names
        region_name_map = {
            'us-east-1': 'US East (N. Virginia)',
            'us-east-2': 'US East (Ohio)',
            'us-west-1': 'US West (N. California)',
            'us-west-2': 'US West (Oregon)',
            'eu-west-1': 'EU (Ireland)',
            'eu-central-1': 'EU (Frankfurt)',
            'ap-southeast-1': 'Asia Pacific (Singapore)',
            'ap-southeast-2': 'Asia Pacific (Sydney)',
            'ap-northeast-1': 'Asia Pacific (Tokyo)',
            'ap-south-1': 'Asia Pacific (Mumbai)',
            'sa-east-1': 'South America (Sao Paulo)',
            'ca-central-1': 'Canada (Central)',
            'eu-west-2': 'EU (London)',
            'eu-west-3': 'EU (Paris)',
            'eu-north-1': 'EU (Stockholm)',
            'ap-northeast-2': 'Asia Pacific (Seoul)',
            'ap-northeast-3': 'Asia Pacific (Osaka)',
            'ap-east-1': 'Asia Pacific (Hong Kong)',
            'me-south-1': 'Middle East (Bahrain)',
            'af-south-1': 'Africa (Cape Town)',
            'eu-south-1': 'EU (Milan)',
        }
        
        region_name = region_name_map.get(region)
        if not region_name:
            print(f"Warning: Unknown region '{region}', cannot fetch pricing")
            return None
        
        # Build the pricing API URL
        # The Pricing API uses a public endpoint that returns JSON
        pricing_url = (
            f"https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/"
            f"region/{region}/index.json"
        )
        
        print(f"Fetching pricing for {instance_type} in {region_name}...")
        print(f"Note: This may take a while as the pricing file is large (~100MB)")
        
        # Fetch the pricing data
        with urllib.request.urlopen(pricing_url, timeout=60) as response:
            pricing_data = json.loads(response.read().decode('utf-8'))
        
        # Search for the instance type in the products
        for product_sku, product in pricing_data.get('products', {}).items():
            attributes = product.get('attributes', {})
            
            # Check if this is the right instance type
            if (attributes.get('instanceType') == instance_type and
                attributes.get('location') == region_name and
                attributes.get('tenancy') == 'Shared' and
                attributes.get('operatingSystem') == 'Linux' and
                attributes.get('preInstalledSw') == 'NA' and
                attributes.get('capacitystatus') == 'Used'):
                
                # Found the product, now get the pricing
                terms = pricing_data.get('terms', {}).get('OnDemand', {})
                product_terms = terms.get(product_sku, {})
                
                for term_sku, term_data in product_terms.items():
                    price_dimensions = term_data.get('priceDimensions', {})
                    
                    for dimension_sku, dimension in price_dimensions.items():
                        price_per_unit = dimension.get('pricePerUnit', {}).get('USD')
                        
                        if price_per_unit:
                            return float(price_per_unit)
        
        print(f"Warning: Could not find pricing for {instance_type} in {region_name}")
        return None
    
    except Exception as e:
        print(f"Warning: Could not fetch EC2 pricing: {e}")
        print("You may need to install boto3 or use AWS CLI for pricing lookup")
        return None


def add_metadata_to_result_file(result_file: str) -> bool:
    """
    Add EC2 metadata to a benchmark result JSON file.
    
    Args:
        result_file: Path to the result JSON file
    
    Returns:
        True if successful, False otherwise
    """
    # Read the existing result file
    try:
        with open(result_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Result file not found: {result_file}")
        return False
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in result file: {e}")
        return False
    
    # Check if metadata already exists
    if 'ec2_instance_type' in data and 'usd_per_hour' in data:
        print(f"✓ EC2 metadata already exists in {result_file}")
        print(f"  Instance Type: {data['ec2_instance_type']}")
        print(f"  USD/hour: ${data['usd_per_hour']}")
        return True
    
    # Fetch EC2 instance type
    instance_type = get_ec2_instance_type()
    if not instance_type:
        print("Error: Could not determine EC2 instance type")
        print("Please add 'ec2_instance_type' and 'usd_per_hour' manually to the result file")
        return False
    
    print(f"✓ Detected EC2 instance type: {instance_type}")
    
    # Fetch EC2 region
    region = get_ec2_region()
    if not region:
        print("Error: Could not determine EC2 region")
        return False
    
    print(f"✓ Detected EC2 region: {region}")
    
    # Fetch pricing
    pricing = get_ec2_pricing(instance_type, region)
    if pricing is None:
        print("Warning: Could not fetch pricing automatically")
        print("Please add 'usd_per_hour' manually to the result file")
        # Still add instance type even if pricing fails
        data['ec2_instance_type'] = instance_type
    else:
        print(f"✓ Fetched pricing: ${pricing}/hour")
        data['ec2_instance_type'] = instance_type
        data['usd_per_hour'] = pricing
    
    # Write back to file
    try:
        with open(result_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✓ Updated {result_file} with EC2 metadata")
        return True
    except Exception as e:
        print(f"Error: Could not write to result file: {e}")
        return False


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 add_ec2_metadata.py <result_file.json>")
        print()
        print("Example:")
        print("  python3 add_ec2_metadata.py duckdb/results/internal/tpch-sf1000-internal-results.json")
        sys.exit(1)
    
    result_file = sys.argv[1]
    
    print(f"=== Adding EC2 Metadata to {result_file} ===")
    print()
    
    success = add_metadata_to_result_file(result_file)
    
    if success:
        print()
        print("✓ Success!")
        sys.exit(0)
    else:
        print()
        print("✗ Failed to add EC2 metadata")
        sys.exit(1)


if __name__ == '__main__':
    main()


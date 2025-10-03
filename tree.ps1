# Save this as Get-ReflexDirTree.ps1 and run from the project root
# Example: .\Get-ReflexDirTree.ps1

# Output file name
$outputFile = "Reflex_DirTree.txt"

# Get current directory
$root = Get-Location

Write-Host "Collecting directory tree from: $root"
Write-Host "Output will be saved to: $outputFile"

# Generate tree
Get-ChildItem -Recurse |
    Sort-Object FullName |
    ForEach-Object {
        # Calculate depth for indentation
        $depth = ($_ | Split-Path -Parent).Replace($root, "").Split("\").Count
        $indent = " " * ($depth * 2)
        "$indent$($_.Name)"
    } | Out-File $outputFile -Encoding UTF8

Write-Host "Directory tree saved to $outputFile"

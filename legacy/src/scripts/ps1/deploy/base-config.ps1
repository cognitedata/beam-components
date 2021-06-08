function Get-Postfix {
    $test_postfix = '-test'
    $prod_postfix = '-prod'

    $Input = Read-Host -Prompt 'Deploy to (T)est, (P)rod or (C)ancel operation?'
    if ($Input -eq 'C' -or $Input -eq 'c') {
        exit
    }
    elseif ($Input -eq 'T' -or $Input -eq 't') {
        return $test_postfix
    } elseif ($Input -eq 'P' -or $Input -eq 'p') {
        return $prod_postfix
    } else {
        Write-Host "Not a valid option. Will exit."
        exit
    }
}
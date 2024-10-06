#![feature(proc_macro_diagnostic)]

use proc_macro::TokenStream;
use quote::ToTokens;
use syn::{punctuated::Punctuated, token::Comma, FnArg, Type};

#[proc_macro_attribute]
pub fn module_interface(_attr: TokenStream, item: TokenStream) -> TokenStream {
	use proc_macro::Diagnostic;
	use std::sync::atomic::{AtomicBool, Ordering};

	// Enforce to only apply to impl blocks
	let input = syn::parse_macro_input!(item as syn::Item);
	let mut input = match input {
		syn::Item::Impl(input) => input,
		_ => {
			return input.to_token_stream().into();
		}
	};

	static CALLED_ONCE: AtomicBool = AtomicBool::new(false);
	if CALLED_ONCE.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
		Diagnostic::spanned(
			proc_macro::Span::call_site(),
			proc_macro::Level::Error,
			"This function can only be called once",
		)
		.emit();
		return input.into_token_stream().into();
	}

	let struct_name = input.self_ty.clone();

	// Get all functions in the impl block
	let functions = input.items.clone().into_iter().filter_map(|item| {
		if let syn::ImplItem::Fn(fun) = item {
			Some(fun)
		} else {
			None
		}
	});

	input.items.push(syn::ImplItem::Fn(syn::parse_quote! {
		pub fn instance(env: Env) -> &'static Self {
			// Unwrap is used here because the instance is always assumed to be correct, and depending on whether it is correct,
			// this would either always fail or never fail, so it makes no sense to write error handling code here.
			env.get_instance_data().unwrap().unwrap()
		}
	}));

	let function_wrappers: Vec<_> = functions
		.filter_map(|fun| {
			if !fun.sig.inputs.iter().any(|x| match x {
				FnArg::Receiver(_) => true,
				_ => false,
			}) {
				return None;
			}

			let mut napi_attr: Option<proc_macro2::TokenStream> = None;

			let mut attrs = fun.attrs.clone();

			if let Some(idx) =
				attrs.iter().position(|attr| attr.path().is_ident("module_interface"))
			{
				let attr = attrs.remove(idx);
				attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("napi") {
						let path = meta.path;
						let remaining = meta.input.cursor().token_stream();
						napi_attr = Some(quote::quote! { #path #remaining });
					};
					Ok(())
				})
				// TODO: fix this, why is it returning an error when it's seemingly working fine?
				.unwrap_or(());
			}

			attrs.retain(|attr| attr.path().is_ident("cfg"));

			let attrs: proc_macro2::TokenStream  = attrs.into_iter().map(|x| x.to_token_stream()).collect();

			let napi_attr = match napi_attr {
				Some(attr) => attr,
				None => {
					return None;
				}
			};

			let name = &fun.sig.ident;
			let params: Punctuated<_, Comma> = fun
				.sig
				.inputs
				.iter()
				.enumerate()
				.filter_map(|(idx, param)| match param {
					FnArg::Receiver(_) => None,
					FnArg::Typed(pat) => {
						match *pat.ty {
							Type::Path(ref path) if path.path.is_ident("Env") => {
								// Skip Env parameter
								return None;
							}
							_ => {}
						}

						let mut new_pat = pat.clone();
						match *new_pat.pat {
							syn::Pat::Ident(ref mut ident) => {
								ident.ident =
									syn::Ident::new(&format!("__param_{idx}"), ident.ident.span());
							}
							_ => {}
						};

						Some(FnArg::Typed(new_pat))
					}
				})
				.collect();
			let env_param = fun.sig.inputs.iter().find_map(|param| match param {
				FnArg::Typed(pat) => match *pat.ty {
					Type::Path(ref path) if path.path.is_ident("Env") => Some(match *pat.pat {
						syn::Pat::Ident(ref ident) => &ident.ident,
						_ => panic!("Expected an identifier"),
					}),
					_ => None,
				},
				_ => None,
			});

			let extra_env_parameter = match env_param {
				Some(e) => vec![e],
				_ => vec![],
			};

			let param_names_only: Punctuated<_, Comma> = extra_env_parameter
				.into_iter()
				.chain(params.iter().map(|param| {
					if let FnArg::Typed(pat) = param {
						match *pat.pat {
							syn::Pat::Ident(ref ident) => &ident.ident,
							_ => panic!("Expected an identifier"),
						}
					} else {
						panic!("Expected a typed argument");
					}
				}))
				.collect();

			let vis = &fun.vis;
			let return_type = &fun.sig.output;

			// let app_arg = match self_param.reference {
			// 	Some(_) => match self_param.mutability {
			// 		Some(_) => {
			// 			quote::quote! { &mut app }
			// 		}
			// 		_ => {
			// 			quote::quote! { &app }
			// 		}
			// 	},
			// 	None => quote::quote! { app },
			// };

			Some(quote::quote! {
				#attrs
				#[#napi_attr]
				#vis fn #name (mut env: Env, #params) #return_type {
					let app = #struct_name::instance(env);
					#struct_name::#name(app, #param_names_only)
				}
			})
		})
		.collect();

	// re-emit the input plus the new code
	TokenStream::from(quote::quote! {
		#input
		#(#function_wrappers)*
	})
}

// #[cfg(test)]
// mod tests {
// 	use super::*;

// 	#[test]
// 	fn it_works() {
// 		let result = add(2, 2);
// 		assert_eq!(result, 4);
// 	}
// }
